package kafka

import (
	"context"
	"time"

	"github.com/Shopify/sarama"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

// concurrentMessageHandler works by fetching messages and pushing them to the channel as it goes.
// Because the messages are received in order, they're introduced in order in the channel.
// When a NACK is received, all the messages after it for the same topic / partition are pushed again.
// It can be configured to have at most N messages in-flight.
type concurrentMessageHandler struct {
	outputChannel chan<- *message.Message

	nackResendSleep time.Duration

	logger      watermill.LoggerAdapter
	closing     chan struct{}
	unmarshaler Unmarshaler
}

func NewConcurrentMessageHandler(
	outputChannel chan<- *message.Message,
	unmarshaler Unmarshaler,
	logger watermill.LoggerAdapter,
	closing chan struct{},
	nackResendSleep time.Duration,
) MessageHandler {
	return &concurrentMessageHandler{
		outputChannel:   outputChannel,
		nackResendSleep: nackResendSleep,
		closing:         closing,
		logger:          logger,
		unmarshaler:     unmarshaler,
	}
}

func (h *concurrentMessageHandler) ProcessMessages(
	ctx context.Context,
	kafkaMessages <-chan *sarama.ConsumerMessage,
	sess sarama.ConsumerGroupSession,
	logFields watermill.LogFields,
) error {
	multiplexing := h.addToMultiplexing(h.closing, h.outputChannel)
	handler := NewMessageHandler(multiplexing, h.unmarshaler, h.logger, h.closing, h.nackResendSleep)
	return handler.ProcessMessages(ctx, kafkaMessages, sess, logFields)
}

func (h *concurrentMessageHandler) addToMultiplexing(closing chan struct{}, outputChannel chan<- *message.Message) chan<- *message.Message {
	multiplexing := make(chan *message.Message)
	go func() {
		defer close(multiplexing)
		for {
			select {
			case <-closing:
				return
			case msg, ok := <-multiplexing:
				if !ok {
					return
				}
				outputChannel <- msg
			}
		}
	}()
	return multiplexing
}
