package kafka

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestBatchMessageHandler(t *testing.T) {
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
				messages, receivedMessages, consumerGroupSession, err := testBatchEventConsumption(
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
					consumerGroupSession.AssertNumberOfCalls(t, "MarkMessage", testConfig.partitionCount)
					for i := 0; i < testConfig.partitionCount; i++ {
						consumerGroupSession.AssertCalled(t, "MarkMessage", messages[testConfig.eventCount-testConfig.partitionCount+i], "")
					}
				}
			})

			t.Run("sends events if less than batch size", func(t *testing.T) {
				testConfig := testConfig{
					batchWait:                10 * time.Millisecond,
					eventCount:               5,
					maxBatchSize:             10,
					partitionCount:           5,
					hasConsumerGroup:         test.hasConsumerGroup,
					hasCountingConsumerGroup: true,
				}
				received := 0
				messages, receivedMessages, consumerGroupSession, err := testBatchEventConsumption(
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
					consumerGroupSession.AssertNumberOfCalls(t, "MarkMessage", testConfig.partitionCount)
					for i := 0; i < testConfig.partitionCount; i++ {
						consumerGroupSession.AssertCalled(t, "MarkMessage", messages[testConfig.eventCount-testConfig.partitionCount+i], "")
					}
				}
			})

			t.Run("processes all events when there are more than the batch size", func(t *testing.T) {
				testConfig := testConfig{
					batchWait:                10 * time.Millisecond,
					eventCount:               15,
					maxBatchSize:             10,
					partitionCount:           5,
					hasConsumerGroup:         test.hasConsumerGroup,
					hasCountingConsumerGroup: true,
				}
				received := 0
				messages, receivedMessages, consumerGroupSession, err := testBatchEventConsumption(
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
					consumerGroupSession.AssertNumberOfCalls(t, "MarkMessage", testConfig.partitionCount*2)
					for i := 0; i < testConfig.partitionCount; i++ {
						consumerGroupSession.AssertCalled(t, "MarkMessage", messages[testConfig.eventCount-testConfig.partitionCount+i], "")
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
				messages, receivedMessages, consumerGroupSession, err := testBatchEventConsumption(
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
				messages, receivedMessages, consumerGroupSession, err := testBatchEventConsumption(
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

			t.Run("NACKed second events results on partial batch", func(t *testing.T) {
				testConfig := testConfig{
					batchWait:                10 * time.Millisecond,
					eventCount:               3,
					maxBatchSize:             10,
					partitionCount:           2,
					hasConsumerGroup:         test.hasConsumerGroup,
					hasCountingConsumerGroup: true,
				}
				received := 0
				messages, receivedMessages, consumerGroupSession, err := testBatchEventConsumption(
					t,
					testConfig,
					func(outputChannel <-chan *message.Message, closing chan<- struct{}) (*message.Message, bool) {
						exit := false
						received++
						select {
						case msg := <-outputChannel:
							if received == testConfig.eventCount {
								msg.Nack()
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
				assertSameEvent(t, receivedMessages[1], messages[1])
				assertSameEvent(t, receivedMessages[2], messages[2])
				// the second event on the first partition is received twice
				assertSameEvent(t, receivedMessages[3], messages[2])
				if test.hasConsumerGroup {
					consumerGroupSession.AssertNumberOfCalls(t, "MarkMessage", 3)
					consumerGroupSession.AssertCalled(t, "MarkMessage", messages[0], "")
					consumerGroupSession.AssertCalled(t, "MarkMessage", messages[1], "")
					consumerGroupSession.AssertCalled(t, "MarkMessage", messages[2], "")
				}
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
				_, receivedMessages, _, err := testBatchEventConsumption(
					t,
					testConfig,
					func(outputChannel <-chan *message.Message, closing chan<- struct{}) (*message.Message, bool) {
						close(closing)
						return nil, true
					})
				assert.NoError(t, err)
				require.Len(t, receivedMessages, 0)
			})
		})
	}
}

type mockConsumerGroupSession struct {
	mock.Mock
}

func newMockConsumerGroupSession() *mockConsumerGroupSession {
	return &mockConsumerGroupSession{}
}

func (cgs *mockConsumerGroupSession) MarkMessage(msg *sarama.ConsumerMessage, metadata string) {
	cgs.Called(msg, metadata)
}

func (cgs *mockConsumerGroupSession) Claims() map[string][]int32 {
	args := cgs.Called()
	return args.Get(0).(map[string][]int32)
}

func (cgs *mockConsumerGroupSession) MemberID() string {
	args := cgs.Called()
	return args.String(0)
}

func (cgs *mockConsumerGroupSession) GenerationID() int32 {
	args := cgs.Called()
	return int32(args.Int(0))
}

func (cgs *mockConsumerGroupSession) MarkOffset(topic string, partition int32, offset int64, metadata string) {
	cgs.Called(topic, partition, offset, metadata)
}

func (cgs *mockConsumerGroupSession) Commit() {
	cgs.Called()
}

func (cgs *mockConsumerGroupSession) ResetOffset(topic string, partition int32, offset int64, metadata string) {
	cgs.Called(topic, partition, offset, metadata)
}

func (cgs *mockConsumerGroupSession) Context() context.Context {
	args := cgs.Called()
	return args.Get(0).(context.Context)
}

type mockConsumerGroupSessionNoCalls struct {
	mockConsumerGroupSession
}

func (cgs *mockConsumerGroupSessionNoCalls) MarkMessage(msg *sarama.ConsumerMessage, metadata string) {
}

type testConfig struct {
	eventCount               int
	batchWait                time.Duration
	nackResendSleep          time.Duration
	maxBatchSize             int16
	partitionCount           int
	hasConsumerGroup         bool
	hasCountingConsumerGroup bool
}

func testBatchEventConsumption(
	t testing.TB,
	testConfig testConfig,
	outputChannelConsumer func(<-chan *message.Message, chan<- struct{}) (*message.Message, bool),
) ([]*sarama.ConsumerMessage, []*message.Message, *mockConsumerGroupSession, error) {
	return testEventConsumption(t, testConfig, func(output chan<- *message.Message, closing chan struct{}) MessageHandler {
		return NewBatchedMessageHandler(
			output,
			DefaultMarshaler{},
			watermill.NopLogger{},
			closing,
			testConfig.maxBatchSize,
			testConfig.batchWait,
			testConfig.nackResendSleep,
		)
	}, outputChannelConsumer)
}

func testEventConsumption(
	t testing.TB,
	testConfig testConfig,
	createHandler func(chan<- *message.Message, chan struct{}) MessageHandler,
	outputChannelConsumer func(<-chan *message.Message, chan<- struct{}) (*message.Message, bool),
) ([]*sarama.ConsumerMessage, []*message.Message, *mockConsumerGroupSession, error) {
	var consumerGroupSession *mockConsumerGroupSession = newMockConsumerGroupSession()
	var sess sarama.ConsumerGroupSession
	if testConfig.hasConsumerGroup {
		if testConfig.hasCountingConsumerGroup {
			sess = consumerGroupSession
			consumerGroupSession.On("MarkMessage", mock.Anything, mock.Anything).Return()
		} else {
			sess = &mockConsumerGroupSessionNoCalls{}
		}
	}
	outputChannel := make(chan *message.Message)
	defer close(outputChannel)
	closing := make(chan struct{})
	handler := createHandler(outputChannel, closing)

	ctx := context.Background()

	kafkaMessages := make(chan *sarama.ConsumerMessage, testConfig.eventCount)
	messages := make([]*sarama.ConsumerMessage, 0)
	for i := 0; i < testConfig.eventCount; i++ {
		msg := generateMessage("topic", i%testConfig.partitionCount, i)
		kafkaMessages <- msg
		messages = append(messages, msg)
	}
	logFields := watermill.LogFields{}
	var mutex sync.Mutex
	receivedMessages := make([]*message.Message, 0, 100)
	go func() {
		for {
			msg, exit := outputChannelConsumer(outputChannel, closing)
			if msg != nil {
				mutex.Lock()
				receivedMessages = append(receivedMessages, msg)
				mutex.Unlock()
			}
			if exit {
				return
			}
		}
	}()
	err := <-handler.ProcessMessages(ctx, kafkaMessages, sess, logFields)
	mutex.Lock()
	defer mutex.Unlock()
	return messages, receivedMessages, consumerGroupSession, err
}

func generateMessage(topic string, partition, offset int) *sarama.ConsumerMessage {
	return &sarama.ConsumerMessage{
		Topic:          "topic",
		Partition:      int32(partition),
		Key:            []byte(fmt.Sprintf("key%d", offset)),
		Value:          []byte(fmt.Sprintf("some-value-%d", offset)),
		Offset:         int64(offset),
		Timestamp:      time.Now(),
		BlockTimestamp: time.Now(),
		Headers: []*sarama.RecordHeader{{
			Key:   []byte(UUIDHeaderKey),
			Value: []byte(watermill.NewUUID()),
		}},
	}
}

func testSameEventsAndSameOrder(t testing.TB, receivedMessages []*message.Message, messages []*sarama.ConsumerMessage) {
	require.Len(t, messages, len(receivedMessages))
	for idx, msg := range receivedMessages {
		assertSameEvent(t, msg, messages[idx])
	}
}

func assertSameEvent(t testing.TB, message *message.Message, kafkaMessage *sarama.ConsumerMessage) {
	assert.Equal(t, string(kafkaMessage.Headers[0].Value), message.UUID)
	offset, _ := MessagePartitionOffsetFromCtx(message.Context())
	assert.Equal(t, offset, kafkaMessage.Offset)
	partition, _ := MessagePartitionFromCtx(message.Context())
	assert.Equal(t, partition, kafkaMessage.Partition)
}
