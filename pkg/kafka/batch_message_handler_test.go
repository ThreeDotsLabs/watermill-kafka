package kafka

import (
	"context"
	"fmt"
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
			name:             "consumer group session is provided",
			hasConsumerGroup: true,
		},
		{
			name:             "no consumer group session",
			hasConsumerGroup: false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Run("consumes all messages in order even if coming from different partitions", func(t *testing.T) {
				testConfig := newTestConfig(test.hasConsumerGroup, 3)
				sess, mock := consumerGroupSession(testConfig)
				closing, outputChannel, handler := testBatchConsumption(testConfig)
				defer close(closing)
				defer close(outputChannel)
				kafkaMessages := make(chan *sarama.ConsumerMessage, 10)
				defer close(kafkaMessages)
				messagesToSend := make([]*sarama.ConsumerMessage, 0, 3)
				messagesToSend = append(messagesToSend, generateMessage("topic1", 0, 0))
				messagesToSend = append(messagesToSend, generateMessage("topic1", 1, 0))
				messagesToSend = append(messagesToSend, generateMessage("topic1", 2, 0))
				for _, msg := range messagesToSend {
					kafkaMessages <- msg
				}
				go func() {
					err := handler.ProcessMessages(context.Background(), kafkaMessages, sess, watermill.LogFields{})
					assert.NoError(t, err)
				}()
				receivedMessages := make([]*message.Message, 0, 3)
				receivedMessages = append(receivedMessages, <-outputChannel)
				receivedMessages = append(receivedMessages, <-outputChannel)
				receivedMessages = append(receivedMessages, <-outputChannel)

				// we do not get more messages because the batch was not processed yet
				waitOrFail(t, 100*time.Millisecond, outputChannel)

				// we ACK the messages
				for _, msg := range receivedMessages {
					msg.Ack()
				}
				testSameMessagesAndLocalOrder(t, receivedMessages, messagesToSend)
				if sess != nil {
					assert.Eventually(t, func() bool { return len(mock.Calls) == 3 }, 1*time.Second, 10*time.Millisecond)
					mock.AssertNumberOfCalls(t, "MarkMessage", 3)
					mock.AssertCalled(t, "MarkMessage", messagesToSend[0], "")
					mock.AssertCalled(t, "MarkMessage", messagesToSend[1], "")
					mock.AssertCalled(t, "MarkMessage", messagesToSend[2], "")
				}
			})

			t.Run("sends messages if less than batch size", func(t *testing.T) {
				testConfig := newTestConfig(test.hasConsumerGroup, 10)
				sess, mock := consumerGroupSession(testConfig)
				closing, outputChannel, handler := testBatchConsumption(testConfig)
				defer close(closing)
				defer close(outputChannel)
				kafkaMessages := make(chan *sarama.ConsumerMessage, 10)
				defer close(kafkaMessages)
				messagesToSend := make([]*sarama.ConsumerMessage, 0, 3)
				messagesToSend = append(messagesToSend, generateMessage("topic1", 0, 0))
				messagesToSend = append(messagesToSend, generateMessage("topic1", 1, 0))
				messagesToSend = append(messagesToSend, generateMessage("topic1", 2, 0))
				for _, msg := range messagesToSend {
					kafkaMessages <- msg
				}
				go func() {
					err := handler.ProcessMessages(context.Background(), kafkaMessages, sess, watermill.LogFields{})
					assert.NoError(t, err)
				}()
				receivedMessages := make([]*message.Message, 0, 3)
				receivedMessages = append(receivedMessages, <-outputChannel)
				receivedMessages = append(receivedMessages, <-outputChannel)
				receivedMessages = append(receivedMessages, <-outputChannel)

				// we do not get more messages because the batch was not processed yet
				waitOrFail(t, 100*time.Millisecond, outputChannel)

				// we ACK the messages
				for _, msg := range receivedMessages {
					msg.Ack()
				}
				testSameMessagesAndLocalOrder(t, receivedMessages, messagesToSend)
				if sess != nil {
					assert.Eventually(t, func() bool { return len(mock.Calls) == 3 }, 1*time.Second, 10*time.Millisecond)
					mock.AssertNumberOfCalls(t, "MarkMessage", 3)
					mock.AssertCalled(t, "MarkMessage", messagesToSend[0], "")
					mock.AssertCalled(t, "MarkMessage", messagesToSend[1], "")
					mock.AssertCalled(t, "MarkMessage", messagesToSend[2], "")
				}
			})

			t.Run("processes all messages when there are more than the batch size", func(t *testing.T) {
				testConfig := newTestConfig(test.hasConsumerGroup, 2)
				sess, mock := consumerGroupSession(testConfig)
				closing, outputChannel, handler := testBatchConsumption(testConfig)
				defer close(closing)
				defer close(outputChannel)
				kafkaMessages := make(chan *sarama.ConsumerMessage, 10)
				defer close(kafkaMessages)
				messagesToSend := make([]*sarama.ConsumerMessage, 0, 3)
				messagesToSend = append(messagesToSend, generateMessage("topic1", 0, 0))
				messagesToSend = append(messagesToSend, generateMessage("topic1", 1, 0))
				messagesToSend = append(messagesToSend, generateMessage("topic1", 2, 0))
				for _, msg := range messagesToSend {
					kafkaMessages <- msg
				}
				go func() {
					err := handler.ProcessMessages(context.Background(), kafkaMessages, sess, watermill.LogFields{})
					assert.NoError(t, err)
				}()
				receivedMessages := make([]*message.Message, 0, 3)
				receivedMessages = append(receivedMessages, <-outputChannel)
				receivedMessages = append(receivedMessages, <-outputChannel)
				// we ACK the messages
				for _, msg := range receivedMessages {
					msg.Ack()
				}
				receivedMessages = append(receivedMessages, <-outputChannel)

				// we do not get more messages because the batch was not processed yet
				waitOrFail(t, 100*time.Millisecond, outputChannel)

				// we ACK the messages
				for _, msg := range receivedMessages {
					msg.Ack()
				}
				testSameMessagesAndLocalOrder(t, receivedMessages, messagesToSend)
				if sess != nil {
					assert.Eventually(t, func() bool { return len(mock.Calls) == 3 }, 1*time.Second, 10*time.Millisecond)
					mock.AssertNumberOfCalls(t, "MarkMessage", 3)
					mock.AssertCalled(t, "MarkMessage", messagesToSend[0], "")
					mock.AssertCalled(t, "MarkMessage", messagesToSend[1], "")
					mock.AssertCalled(t, "MarkMessage", messagesToSend[2], "")
				}
			})

			t.Run("NACKed messages are re-sent (and following messages too)", func(t *testing.T) {
				testConfig := newTestConfig(test.hasConsumerGroup, 10)
				sess, mock := consumerGroupSession(testConfig)
				closing, outputChannel, handler := testBatchConsumption(testConfig)
				defer close(closing)
				defer close(outputChannel)
				kafkaMessages := make(chan *sarama.ConsumerMessage, 10)
				defer close(kafkaMessages)
				messagesToSend := make([]*sarama.ConsumerMessage, 0, 4)
				messagesToSend = append(messagesToSend, generateMessage("topic1", 0, 0))
				messagesToSend = append(messagesToSend, generateMessage("topic1", 0, 1))
				messagesToSend = append(messagesToSend, generateMessage("topic1", 1, 0))
				messagesToSend = append(messagesToSend, generateMessage("topic1", 1, 1))
				for _, msg := range messagesToSend {
					kafkaMessages <- msg
				}
				go func() {
					err := handler.ProcessMessages(context.Background(), kafkaMessages, sess, watermill.LogFields{})
					assert.NoError(t, err)
				}()
				receivedMessages := make([]*message.Message, 0, 4)
				receivedMessages = append(receivedMessages, <-outputChannel)
				receivedMessages = append(receivedMessages, <-outputChannel)
				receivedMessages = append(receivedMessages, <-outputChannel)
				receivedMessages = append(receivedMessages, <-outputChannel)
				receivedMessages[0].Ack()
				receivedMessages[1].Ack()
				receivedMessages[2].Nack()
				receivedMessages[3].Ack()
				receivedMessages = receivedMessages[:0]
				receivedMessages = append(receivedMessages, <-outputChannel)
				receivedMessages = append(receivedMessages, <-outputChannel)

				// we do not get more messages because the batch was not processed yet
				waitOrFail(t, 100*time.Millisecond, outputChannel)

				receivedMessages[0].Ack()
				receivedMessages[1].Ack()
				testSameMessagesAndLocalOrder(t, receivedMessages, messagesToSend[2:])
				if sess != nil {
					assert.Eventually(t, func() bool { return len(mock.Calls) == 2 }, 1*time.Second, 10*time.Millisecond)
					mock.AssertNumberOfCalls(t, "MarkMessage", 2)
					mock.AssertCalled(t, "MarkMessage", messagesToSend[1], "")
					mock.AssertCalled(t, "MarkMessage", messagesToSend[3], "")
				}
			})

			t.Run("ACKed message are not re-sent if a NACKed follows", func(t *testing.T) {
				testConfig := newTestConfig(test.hasConsumerGroup, 10)
				sess, mock := consumerGroupSession(testConfig)
				closing, outputChannel, handler := testBatchConsumption(testConfig)
				defer close(closing)
				defer close(outputChannel)
				kafkaMessages := make(chan *sarama.ConsumerMessage, 10)
				defer close(kafkaMessages)
				messagesToSend := make([]*sarama.ConsumerMessage, 0, 3)
				messagesToSend = append(messagesToSend, generateMessage("topic1", 0, 0))
				messagesToSend = append(messagesToSend, generateMessage("topic1", 0, 1))
				messagesToSend = append(messagesToSend, generateMessage("topic1", 0, 2))
				for _, msg := range messagesToSend {
					kafkaMessages <- msg
				}
				go func() {
					err := handler.ProcessMessages(context.Background(), kafkaMessages, sess, watermill.LogFields{})
					assert.NoError(t, err)
				}()
				receivedMessages := make([]*message.Message, 0, 3)
				receivedMessages = append(receivedMessages, <-outputChannel)
				receivedMessages = append(receivedMessages, <-outputChannel)
				receivedMessages = append(receivedMessages, <-outputChannel)
				receivedMessages[0].Ack()
				receivedMessages[1].Nack()
				receivedMessages[2].Ack()
				receivedMessages = receivedMessages[:0]
				receivedMessages = append(receivedMessages, <-outputChannel)
				receivedMessages = append(receivedMessages, <-outputChannel)
				// we do not get more messages because the batch was not processed yet
				waitOrFail(t, 100*time.Millisecond, outputChannel)
				receivedMessages[0].Ack()
				receivedMessages[1].Ack()
				testSameMessagesAndLocalOrder(t, receivedMessages, messagesToSend[1:])
				if sess != nil {
					assert.Eventually(t, func() bool { return len(mock.Calls) == 2 }, 1*time.Second, 10*time.Millisecond)
					mock.AssertNumberOfCalls(t, "MarkMessage", 2)
					mock.AssertCalled(t, "MarkMessage", messagesToSend[0], "")
					mock.AssertCalled(t, "MarkMessage", messagesToSend[2], "")
				}
			})

			t.Run("NACK wait period is applied", func(t *testing.T) {
				testConfig := newTestConfig(test.hasConsumerGroup, 10)
				testConfig.nackResendSleep = 500 * time.Millisecond
				sess, mock := consumerGroupSession(testConfig)
				closing, outputChannel, handler := testBatchConsumption(testConfig)
				defer close(closing)
				defer close(outputChannel)
				kafkaMessages := make(chan *sarama.ConsumerMessage, 10)
				defer close(kafkaMessages)
				messagesToSend := make([]*sarama.ConsumerMessage, 0, 1)
				messagesToSend = append(messagesToSend, generateMessage("topic1", 0, 0))
				for _, msg := range messagesToSend {
					kafkaMessages <- msg
				}
				go func() {
					err := handler.ProcessMessages(context.Background(), kafkaMessages, sess, watermill.LogFields{})
					assert.NoError(t, err)
				}()
				receivedMessages := make([]*message.Message, 0, 1)
				receivedMessages = append(receivedMessages, <-outputChannel)
				receivedMessages[0].Nack()
				waitOrFail(t, 400*time.Millisecond, outputChannel)
				receivedMessages = receivedMessages[:0]
				receivedMessages = append(receivedMessages, <-outputChannel)
				receivedMessages[0].Ack()
				testSameMessagesAndLocalOrder(t, receivedMessages, messagesToSend)
				if sess != nil {
					assert.Eventually(t, func() bool { return len(mock.Calls) == 1 }, 1*time.Second, 10*time.Millisecond)
					mock.AssertNumberOfCalls(t, "MarkMessage", 1)
					mock.AssertCalled(t, "MarkMessage", messagesToSend[0], "")
				}
			})
		})

		t.Run("closing without messages", func(t *testing.T) {
			testConfig := newTestConfig(test.hasConsumerGroup, 10)
			closing, outputChannel, handler := testBatchConsumption(testConfig)
			defer close(outputChannel)

			kafkaMessages := make(chan *sarama.ConsumerMessage, 10)
			defer close(kafkaMessages)
			go func() {
				err := handler.ProcessMessages(context.Background(), kafkaMessages, nil, watermill.LogFields{})
				assert.NoError(t, err)
			}()
			close(closing)
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
	batchWait                time.Duration
	nackResendSleep          time.Duration
	maxBatchSize             int16
	hasConsumerGroup         bool
	hasCountingConsumerGroup bool
}

func testBatchConsumption(
	testConfig testConfig,
) (chan<- struct{}, chan *message.Message, MessageHandler) {
	outputChannel := make(chan *message.Message)
	closing := make(chan struct{})
	handler := NewBatchedMessageHandler(
		outputChannel,
		DefaultMarshaler{},
		watermill.NopLogger{},
		closing,
		testConfig.maxBatchSize,
		testConfig.batchWait,
		testConfig.nackResendSleep,
	)
	return closing, outputChannel, handler
}

func consumerGroupSession(testConfig testConfig) (sarama.ConsumerGroupSession, *mock.Mock) {
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
	return sess, &consumerGroupSession.Mock
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

func testSameMessagesAndLocalOrder(t testing.TB, receivedMessages []*message.Message, messages []*sarama.ConsumerMessage) {
	require.Len(t, messages, len(receivedMessages))
	messagesPerPartition := make(map[int32][]*message.Message)
	consumerMessagesPerPartition := make(map[int32][]*sarama.ConsumerMessage)
	for i := 0; i < len(receivedMessages); i++ {
		consumerMessage := messages[i]
		partition := consumerMessage.Partition
		consumerMessages, ok := consumerMessagesPerPartition[partition]
		if !ok {
			consumerMessages = make([]*sarama.ConsumerMessage, 0, 1)
		}
		consumerMessages = append(consumerMessages, consumerMessage)
		consumerMessagesPerPartition[partition] = consumerMessages

		kafkaMessage := receivedMessages[i]
		partition, _ = MessagePartitionFromCtx(kafkaMessage.Context())
		messages, ok := messagesPerPartition[partition]
		if !ok {
			messages = make([]*message.Message, 0, 1)
		}
		messages = append(messages, kafkaMessage)
		messagesPerPartition[partition] = messages
	}
	require.Equal(t, len(messagesPerPartition), len(consumerMessagesPerPartition))
	for partition, messages := range messagesPerPartition {
		if consumerMessages, ok := consumerMessagesPerPartition[partition]; ok {
			require.Len(t, messages, len(consumerMessages))
			for idx, msg := range messages {
				assertSameMessage(t, msg, consumerMessages[idx])
			}
		} else {
			t.Fatal(fmt.Sprintf("No messages for partition: %d", partition))
			t.Fail()
		}
	}
}

func assertSameMessage(t testing.TB, message *message.Message, kafkaMessage *sarama.ConsumerMessage) {
	assert.Equal(t, string(kafkaMessage.Headers[0].Value), message.UUID)
	offset, _ := MessagePartitionOffsetFromCtx(message.Context())
	assert.Equal(t, offset, kafkaMessage.Offset)
	partition, _ := MessagePartitionFromCtx(message.Context())
	assert.Equal(t, partition, kafkaMessage.Partition)
}

func waitOrFail(t testing.TB, duration time.Duration, outputChannel <-chan *message.Message) {
	select {
	case <-outputChannel:
		t.Fatal("should not receive an message yet")
		t.FailNow()
	case <-time.After(duration):
	}
}

func newTestConfig(hasConsumerGroup bool, batchSize int) testConfig {
	return testConfig{
		batchWait:                10 * time.Millisecond,
		maxBatchSize:             int16(batchSize),
		hasConsumerGroup:         hasConsumerGroup,
		hasCountingConsumerGroup: true,
	}
}
