package kafka

import (
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/stretchr/testify/assert"
)

func BenchmarkBatchMessageHandler(b *testing.B) {
	tests := []struct {
		name                     string
		hasConsumerGroup         bool
		hasCountingConsumerGroup bool
	}{
		{
			name:             "no consumer group session",
			hasConsumerGroup: false,
		},
		{
			name:                     "consumer group session is provided",
			hasConsumerGroup:         true,
			hasCountingConsumerGroup: false,
		},
	}
	for _, test := range tests {
		b.Run(test.name, func(b *testing.B) {
			b.Run("consumes all events in order", func(b *testing.B) {
				testConfig := testConfig{
					batchWait:                100 * time.Millisecond,
					eventCount:               10000,
					maxBatchSize:             10,
					partitionCount:           5,
					hasConsumerGroup:         test.hasConsumerGroup,
					hasCountingConsumerGroup: test.hasCountingConsumerGroup,
				}
				received := 0
				messages, receivedMessages, _, err := testBatchEventConsumption(
					b,
					testConfig,
					func(outputChannel <-chan *message.Message, closing chan<- struct{}) (*message.Message, bool) {
						exit := false
						select {
						case msg := <-outputChannel:
							received++
							msg.Ack()
							if received == testConfig.eventCount {
								exit = true
								close(closing)
							}
							return msg, exit
						}
					})
				assert.NoError(b, err)
				testSameEventsAndSameOrder(b, receivedMessages, messages)
			})

		})
	}
}
