package kafka

import (
	"context"
	"time"

	"github.com/Shopify/sarama"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/pkg/errors"
)

type messageHandler struct {
	outputChannel chan<- *message.Message
	unmarshaler   Unmarshaler

	nackResendSleep time.Duration

	logger  watermill.LoggerAdapter
	closing chan struct{}
}

func (h messageHandler) processMessage(
	ctx context.Context,
	kafkaMsg *sarama.ConsumerMessage,
	sess sarama.ConsumerGroupSession,
	messageLogFields watermill.LogFields,
) error {
	receivedMsgLogFields := messageLogFields.Add(watermill.LogFields{
		"kafka_partition_offset": kafkaMsg.Offset,
		"kafka_partition":        kafkaMsg.Partition,
	})

	h.logger.Trace("Received message from Kafka", receivedMsgLogFields)

	ctx = setPartitionToCtx(ctx, kafkaMsg.Partition)
	ctx = setPartitionOffsetToCtx(ctx, kafkaMsg.Offset)
	ctx = setMessageTimestampToCtx(ctx, kafkaMsg.Timestamp)
	ctx = setMessageKeyToCtx(ctx, kafkaMsg.Key)

	msg, err := h.unmarshaler.Unmarshal(kafkaMsg)
	if err != nil {
		// resend will make no sense, stopping consumerGroupHandler
		return errors.Wrap(err, "message unmarshal failed")
	}

	ctx, cancelCtx := context.WithCancel(ctx)
	msg.SetContext(ctx)
	defer cancelCtx()

	receivedMsgLogFields = receivedMsgLogFields.Add(watermill.LogFields{
		"message_uuid": msg.UUID,
	})

ResendLoop:
	for {
		select {
		case h.outputChannel <- msg:
			h.logger.Trace("Message sent to consumer", receivedMsgLogFields)
		case <-h.closing:
			h.logger.Trace("Closing, message discarded", receivedMsgLogFields)
			return nil
		case <-ctx.Done():
			h.logger.Trace("Closing, ctx cancelled before sent to consumer", receivedMsgLogFields)
			return nil
		}

		select {
		case <-msg.Acked():
			if sess != nil {
				sess.MarkMessage(kafkaMsg, "")
			}
			h.logger.Trace("Message Acked", receivedMsgLogFields)
			break ResendLoop
		case <-msg.Nacked():
			h.logger.Trace("Message Nacked", receivedMsgLogFields)

			// reset acks, etc.
			msg = msg.Copy()
			if h.nackResendSleep != NoSleep {
				time.Sleep(h.nackResendSleep)
			}

			continue ResendLoop
		case <-h.closing:
			h.logger.Trace("Closing, message discarded before ack", receivedMsgLogFields)
			return nil
		case <-ctx.Done():
			h.logger.Trace("Closing, ctx cancelled before ack", receivedMsgLogFields)
			return nil
		}
	}

	return nil
}
