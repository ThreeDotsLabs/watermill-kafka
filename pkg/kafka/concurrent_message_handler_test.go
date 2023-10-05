package kafka

import (
	"context"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestConcurrentMessageHandler(t *testing.T) {
	tests := []struct {
		name             string
		hasConsumerGroup bool
	}{
		{
			name:             "no consumer group session",
			hasConsumerGroup: false,
		},
		{
			name:             "consumer group session is provided",
			hasConsumerGroup: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Run("consumes all events in order", func(t *testing.T) {
				testConfig := testConfig{
					batchWait:                10 * time.Millisecond,
					eventCount:               10,
					maxBatchSize:             10,
					partitionCount:           5,
					hasConsumerGroup:         test.hasConsumerGroup,
					hasCountingConsumerGroup: true,
				}
				received := 0
				messages, receivedMessages, consumerGroupSession, err := testConcurrentEventConsumption(
					t,
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
				assert.NoError(t, err)
				testSameEventsAndSameOrder(t, receivedMessages, messages)
				if test.hasConsumerGroup {
					consumerGroupSession.AssertNumberOfCalls(t, "MarkMessage", len(messages))
					for i := 0; i < len(messages); i++ {
						consumerGroupSession.AssertCalled(t, "MarkMessage", messages[i%testConfig.eventCount], "")
					}
				}
			})

			t.Run("NACKed events are re-sent", func(t *testing.T) {
				testConfig := testConfig{
					batchWait:                10 * time.Millisecond,
					eventCount:               1,
					maxBatchSize:             10,
					partitionCount:           5,
					hasConsumerGroup:         test.hasConsumerGroup,
					hasCountingConsumerGroup: true,
				}
				received := 0
				nacked := false
				messages, receivedMessages, consumerGroupSession, err := testConcurrentEventConsumption(
					t,
					testConfig,
					func(outputChannel <-chan *message.Message, closing chan<- struct{}) (*message.Message, bool) {
						exit := false
						received++
						select {
						case msg := <-outputChannel:
							if !nacked {
								msg.Nack()
								nacked = true
							} else {
								msg.Ack()
							}
							if received == testConfig.eventCount+1 {
								exit = true
								close(closing)
							}
							return msg, exit
						}
					})
				assert.NoError(t, err)
				require.Len(t, receivedMessages, len(messages)+1)
				assertSameEvent(t, receivedMessages[0], messages[0])
				assertSameEvent(t, receivedMessages[1], messages[0])
				if test.hasConsumerGroup {
					consumerGroupSession.AssertNumberOfCalls(t, "MarkMessage", 1)
					consumerGroupSession.AssertCalled(t, "MarkMessage", messages[0], "")
				}
			})

			t.Run("NACK wait period is applied", func(t *testing.T) {
				testConfig := testConfig{
					batchWait:                10 * time.Millisecond,
					eventCount:               1,
					maxBatchSize:             10,
					partitionCount:           5,
					hasConsumerGroup:         test.hasConsumerGroup,
					nackResendSleep:          1 * time.Second,
					hasCountingConsumerGroup: true,
				}
				received := 0
				nacked := false
				start := time.Now()
				var ackTime time.Time
				messages, receivedMessages, consumerGroupSession, err := testConcurrentEventConsumption(
					t,
					testConfig,
					func(outputChannel <-chan *message.Message, closing chan<- struct{}) (*message.Message, bool) {
						exit := false
						received++
						select {
						case msg := <-outputChannel:
							if !nacked {
								msg.Nack()
								nacked = true
							} else {
								ackTime = time.Now()
								msg.Ack()
							}
							if received == testConfig.eventCount+1 {
								exit = true
								close(closing)
							}
							return msg, exit
						}
					})
				assert.NoError(t, err)
				require.Len(t, receivedMessages, len(messages)+1)
				assertSameEvent(t, receivedMessages[0], messages[0])
				assertSameEvent(t, receivedMessages[1], messages[0])
				if test.hasConsumerGroup {
					consumerGroupSession.AssertNumberOfCalls(t, "MarkMessage", 1)
					consumerGroupSession.AssertCalled(t, "MarkMessage", messages[0], "")
				}
				assert.GreaterOrEqual(t, ackTime.Sub(start), 1*time.Second)
			})

			t.Run("closing without events", func(t *testing.T) {
				testConfig := testConfig{
					batchWait:                10 * time.Millisecond,
					eventCount:               0,
					maxBatchSize:             10,
					partitionCount:           2,
					hasConsumerGroup:         test.hasConsumerGroup,
					hasCountingConsumerGroup: true,
				}
				_, receivedMessages, _, err := testConcurrentEventConsumption(
					t,
					testConfig,
					func(outputChannel <-chan *message.Message, closing chan<- struct{}) (*message.Message, bool) {
						close(closing)
						return nil, true
					})
				assert.NoError(t, err)
				require.Len(t, receivedMessages, 0)
			})

			t.Run("can process multiple concurrent", func(t *testing.T) {
				var consumerGroupSession *mockConsumerGroupSession = newMockConsumerGroupSession()
				var sess sarama.ConsumerGroupSession
				if test.hasConsumerGroup {
					sess = consumerGroupSession
					consumerGroupSession.On("MarkMessage", mock.Anything, mock.Anything).Return()
				}
				outputChannel := make(chan *message.Message)
				defer close(outputChannel)
				closing := make(chan struct{})
				defer close(closing)
				handler := NewConcurrentMessageHandler(
					outputChannel,
					DefaultMarshaler{},
					watermill.NopLogger{},
					closing,
					0,
				)
				logFields := watermill.LogFields{}
				partitionOneChannel := make(chan *sarama.ConsumerMessage, 2)
				partition1Message1 := generateMessage("topic", 1, 1)
				partition1Message2 := generateMessage("topic", 1, 2)
				partitionOneChannel <- partition1Message1
				partitionOneChannel <- partition1Message2
				partitionTwoChannel := make(chan *sarama.ConsumerMessage, 2)
				partition2Message1 := generateMessage("topic", 2, 1)
				partition2Message2 := generateMessage("topic", 2, 2)
				partitionTwoChannel <- partition2Message1
				partitionTwoChannel <- partition2Message2
				ctx := context.Background()
				go func() {
					handler.ProcessMessages(ctx, partitionOneChannel, sess, logFields)
				}()
				go func() {
					handler.ProcessMessages(ctx, partitionTwoChannel, sess, logFields)
				}()

				firstMessage := <-outputChannel
				offset, ok := MessagePartitionOffsetFromCtx(firstMessage.Context())
				assert.Equal(t, true, ok)
				assert.Equal(t, int64(1), offset)
				secondMessage := <-outputChannel
				offset, ok = MessagePartitionOffsetFromCtx(secondMessage.Context())
				assert.Equal(t, true, ok)
				assert.Equal(t, int64(1), offset)
				select {
				case <-outputChannel:
					t.Fatal("should wait for ACKs")
					t.FailNow()
				case <-time.After(500 * time.Millisecond):
				}
				firstMessage.Ack()
				thirdMessage := <-outputChannel
				offset, ok = MessagePartitionOffsetFromCtx(thirdMessage.Context())
				assert.Equal(t, true, ok)
				assert.Equal(t, int64(2), offset)
				secondMessage.Ack()
				fourthMessage := <-outputChannel
				offset, ok = MessagePartitionOffsetFromCtx(fourthMessage.Context())
				assert.Equal(t, true, ok)
				assert.Equal(t, int64(2), offset)
			})
		})
	}
}

func testConcurrentEventConsumption(
	t testing.TB,
	testConfig testConfig,
	outputChannelConsumer func(<-chan *message.Message, chan<- struct{}) (*message.Message, bool),
) ([]*sarama.ConsumerMessage, []*message.Message, *mockConsumerGroupSession, error) {
	return testEventConsumption(t, testConfig, func(output chan<- *message.Message, closing chan struct{}) MessageHandler {
		return NewConcurrentMessageHandler(
			output,
			DefaultMarshaler{},
			watermill.NopLogger{},
			closing,
			testConfig.nackResendSleep,
		)
	}, outputChannelConsumer)
}
