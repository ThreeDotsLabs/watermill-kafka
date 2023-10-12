package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/Shopify/sarama"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

// batchedMessageHandler works by fetching up to N events from the provided channel
// waiting until maxWaitTime.
// It then takes the collected KafkaMessages and pushes them in order to outputChannel.
// When all have been ACKed or NACKed, it updates the offsets with the highest ACKed
// for each involved partition.
type batchedMessageHandler struct {
	outputChannel chan<- *message.Message

	maxBatchSize int16
	maxWaitTime  time.Duration

	nackResendSleep time.Duration

	logger        watermill.LoggerAdapter
	closing       chan struct{}
	messageParser messageParser
	messages      chan *messageHolder
}

func NewBatchedMessageHandler(
	outputChannel chan<- *message.Message,
	unmarshaler Unmarshaler,
	logger watermill.LoggerAdapter,
	closing chan struct{},
	maxBatchSize int16,
	maxWaitTime time.Duration,
	nackResendSleep time.Duration,
) MessageHandler {
	handler := &batchedMessageHandler{
		outputChannel:   outputChannel,
		maxBatchSize:    maxBatchSize,
		maxWaitTime:     maxWaitTime,
		nackResendSleep: nackResendSleep,
		closing:         closing,
		logger:          logger,
		messageParser: messageParser{
			unmarshaler: unmarshaler,
		},
		messages: make(chan *messageHolder, 0),
	}
	go handler.startProcessing()
	return handler
}

func (h *batchedMessageHandler) startProcessing() {
	buffer := make([]*messageHolder, 0, h.maxBatchSize)
	mustSleep := h.nackResendSleep != NoSleep
	logFields := watermill.LogFields{}
	sendDeadline := time.Now().Add(h.maxWaitTime)
	timer := time.NewTimer(h.maxWaitTime)
	for {
		timerExpired := false
		select {
		case message, ok := <-h.messages:
			if !ok {
				h.logger.Debug("Messages channel is closed", logFields)
			}
			buffer = append(buffer, message)
		case <-timer.C:
			if len(buffer) > 0 {
				h.logger.Trace("Timer expired, sending already fetched events.", logFields)
			}
			timerExpired = true
			break
		case <-h.closing:
			h.logger.Debug("Subscriber is closing, stopping messageHandler", logFields)
			return
		}
		size := len(buffer)
		if (timerExpired && size > 0) || size == int(h.maxBatchSize) {
			sendDeadline = time.Now().Add(h.maxWaitTime)
			timerExpired = false
			newBuffer, err := h.processBatch(buffer)
			if err != nil {
				return
			}
			if newBuffer == nil {
				return
			}
			buffer = newBuffer
			// if there are events in the buffer, it means there was NACKs, so we wait
			if len(buffer) > 0 && mustSleep {
				time.Sleep(h.nackResendSleep)
			}
		}
		timer.Reset(sendDeadline.Sub(time.Now()))
	}
}

func (h *batchedMessageHandler) ProcessMessages(
	ctx context.Context,
	kafkaMessages <-chan *sarama.ConsumerMessage,
	sess sarama.ConsumerGroupSession,
	logFields watermill.LogFields,
) error {
	for {
		select {
		case kafkaMsg := <-kafkaMessages:
			if kafkaMsg == nil {
				h.logger.Debug("kafkaMsg is closed, stopping ProcessMessages", logFields)
				return nil
			}
			msg, err := h.messageParser.prepareAndProcessMessage(ctx, kafkaMsg, h.logger, logFields, sess)
			if err != nil {
				return err
			}
			h.messages <- msg
		case <-h.closing:
			h.logger.Debug("Subscriber is closing, stopping messageHandler", logFields)
			return nil
		case <-ctx.Done():
			h.logger.Debug("Ctx was cancelled, stopping messageHandler", logFields)
			return nil
		}
	}
}

func (h *batchedMessageHandler) processBatch(
	buffer []*messageHolder,
) ([]*messageHolder, error) {
	waitChannels := make([]<-chan bool, 0, len(buffer))
	for _, msgHolder := range buffer {
		ctx, cancelCtx := context.WithCancel(msgHolder.message.Context())
		msgHolder.message.SetContext(ctx)
		select {
		case h.outputChannel <- msgHolder.message:
			h.logger.Trace("Message sent to consumer", msgHolder.logFields)
			waitChannels = append(waitChannels, waitForMessage(ctx, h.logger, msgHolder, cancelCtx))
		case <-h.closing:
			h.logger.Trace("Closing, message discarded", msgHolder.logFields)
			defer cancelCtx()
			return nil, nil
		case <-ctx.Done():
			h.logger.Trace("Closing, ctx cancelled before message was sent to consumer", msgHolder.logFields)
			defer cancelCtx()
			return nil, nil
		}
	}

	// we wait for all the events to be ACKed or NACKed
	// and we store for each partition the last message that was ACKed so we
	// can mark the latest complete offset
	lastComittableMessages := make(map[string]*messageHolder, 0)
	nackedPartitions := make(map[string]struct{})
	newBuffer := make([]*messageHolder, 0, h.maxBatchSize)
	for idx, waitChannel := range waitChannels {
		msgHolder := buffer[idx]
		h.logger.Trace("Waiting for event to be acked", msgHolder.logFields)
		select {
		case ack, ok := <-waitChannel:
			h.logger.Info("Received ACK / NACK response or closed", msgHolder.logFields)
			// it was aborted
			if !ok {
				h.logger.Info("Returning as messages were closed", msgHolder.logFields)
				return nil, nil
			}
			topicAndPartition := fmt.Sprintf("%s-%d", msgHolder.kafkaMessage.Topic, msgHolder.kafkaMessage.Partition)
			_, partitionNacked := nackedPartitions[topicAndPartition]
			if !ack || partitionNacked {
				newBuffer = append(newBuffer, msgHolder.Copy())
				nackedPartitions[topicAndPartition] = struct{}{}
				break
			}
			if !partitionNacked && ack {
				lastComittableMessages[topicAndPartition] = msgHolder
			}
		}
	}

	// If a session is provided, we mark the latest committable message for
	// each partition as done. This is required, because if we did not mark anything we might re-process
	// events unnecessarily. If we marked the latest in the bulk, we could lose NACKed messages.
	for _, lastComittable := range lastComittableMessages {
		if lastComittable.sess != nil {
			h.logger.Trace("Marking offset as complete for", lastComittable.logFields)
			lastComittable.sess.MarkMessage(lastComittable.kafkaMessage, "")
		}
	}

	return newBuffer, nil
}
