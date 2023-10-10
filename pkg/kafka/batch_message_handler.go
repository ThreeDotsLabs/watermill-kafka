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
	return &batchedMessageHandler{
		outputChannel:   outputChannel,
		maxBatchSize:    maxBatchSize,
		maxWaitTime:     maxWaitTime,
		nackResendSleep: nackResendSleep,
		closing:         closing,
		logger:          logger,
		messageParser: messageParser{
			unmarshaler: unmarshaler,
		},
	}
}

func (h *batchedMessageHandler) ProcessMessages(
	ctx context.Context,
	kafkaMessages <-chan *sarama.ConsumerMessage,
	sess sarama.ConsumerGroupSession,
	logFields watermill.LogFields,
) <-chan error {
	finish := make(chan error)
	go func() {
		buffer := make([]*messageHolder, 0, h.maxBatchSize)
		defer close(finish)
		mustSleep := h.nackResendSleep != NoSleep
	EventProcessingLoop:
		timer := time.After(h.maxWaitTime)
		timerExpired := false
		for {
			select {
			case kafkaMsg := <-kafkaMessages:
				if kafkaMsg == nil {
					h.logger.Debug("kafkaMsg is closed, stopping ProcessMessages", logFields)
					return
				}
				msg, err := h.messageParser.prepareAndProcessMessage(ctx, kafkaMsg, h.logger, logFields)
				if err != nil {
					finish <- err
					return
				}
				buffer = append(buffer, msg)
			case <-timer:
				if len(buffer) > 0 {
					h.logger.Trace("Timer expired, sending already fetched events.", logFields)
				}
				timerExpired = true
				break
			case <-h.closing:
				h.logger.Debug("Subscriber is closing, stopping messageHandler", logFields)
				return
			case <-ctx.Done():
				h.logger.Debug("Ctx was cancelled, stopping messageHandler", logFields)
				return
			}
			size := len(buffer)
			if (timerExpired && size > 0) || size == int(h.maxBatchSize) {
				timerExpired = false
				newBuffer, err := h.processBatch(ctx, buffer, sess)
				if err != nil {
					finish <- err
					break
				}
				if newBuffer == nil {
					break
				}
				buffer = newBuffer
				// if there are events in the buffer, it means there was NACKs, so we wait
				if len(buffer) > 0 && mustSleep {
					time.Sleep(h.nackResendSleep)
				}
			}
			goto EventProcessingLoop
		}
	}()
	return finish
}

func (h *batchedMessageHandler) processBatch(
	ctx context.Context,
	buffer []*messageHolder,
	sess sarama.ConsumerGroupSession,
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
			if !ack {
				newBuffer = append(newBuffer, msgHolder.Copy())
				nackedPartitions[topicAndPartition] = struct{}{}
				break
			}
			if _, partitionNacked := nackedPartitions[topicAndPartition]; !partitionNacked && ack {
				lastComittableMessages[topicAndPartition] = msgHolder
			}
		}
	}

	// If a session is provided, we mark the latest committable message for
	// each partition as done. This is required, because if we did not mark anything we might re-process
	// events unnecessarily. If we marked the latest in the bulk, we could lose NACKed messages.
	if sess != nil {
		for _, lastComittable := range lastComittableMessages {
			h.logger.Trace("Marking offset as complete for", lastComittable.logFields)
			sess.MarkMessage(lastComittable.kafkaMessage, "")
		}
	}

	return newBuffer, nil
}
