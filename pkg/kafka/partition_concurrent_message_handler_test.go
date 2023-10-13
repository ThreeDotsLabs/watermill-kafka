package kafka

import (
	"context"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/stretchr/testify/assert"
)

func TestPartitionConcurrentMessageHandler(t *testing.T) {
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
			t.Run("consumes all messages in order even if coming from different partitions", func(t *testing.T) {
				testConfig := newTestConfig(test.hasConsumerGroup, 3)
				sess, mock := consumerGroupSession(testConfig)
				closing, outputChannel, handler := testPartitionConcurrentConsumption(testConfig)
				defer close(closing)
				defer close(outputChannel)
				partitions := 3
				messagesPerPartition := 3
				messagesToSend := make([]*sarama.ConsumerMessage, 0, 3)
				for partition := 0; partition < partitions; partition++ {
					kafkaMessages := make(chan *sarama.ConsumerMessage, 10)
					for offset := 0; offset < messagesPerPartition; offset++ {
						msg := generateMessage("topic1", partition, offset)
						messagesToSend = append(messagesToSend, msg)
						kafkaMessages <- msg
					}
					go func() {
						inputChannel := kafkaMessages
						defer close(inputChannel)
						err := handler.ProcessMessages(context.Background(), inputChannel, sess, watermill.LogFields{})
						assert.NoError(t, err)
					}()
				}
				receivedMessages := make([]*message.Message, 0, 3)
				for i := 0; i < messagesPerPartition; i++ {
					for j := 0; j < partitions; j++ {
						receivedMessages = append(receivedMessages, <-outputChannel)
					}

					// we do not get more messages because the batch was not processed yet
					waitOrFail(t, 100*time.Millisecond, outputChannel)

					// we ACK the messages
					for _, msg := range receivedMessages {
						msg.Ack()
					}
				}

				testSameMessagesAndLocalOrder(t, receivedMessages, messagesToSend)
				if sess != nil {
					assert.Eventually(t, func() bool { return len(mock.Calls) == 9 }, 1*time.Second, 10*time.Millisecond)
					mock.AssertNumberOfCalls(t, "MarkMessage", 9)
					for currentMessage := 0; currentMessage < messagesPerPartition; currentMessage++ {
						for currentPartition := 0; currentPartition < partitions; currentPartition++ {
							mock.AssertCalled(t, "MarkMessage", messagesToSend[messagesPerPartition*currentPartition+currentMessage], "")
						}
					}
				}
			})

			t.Run("NACKed messages are re-sent", func(t *testing.T) {
				testConfig := newTestConfig(test.hasConsumerGroup, 3)
				sess, mock := consumerGroupSession(testConfig)
				closing, outputChannel, handler := testPartitionConcurrentConsumption(testConfig)
				defer close(closing)
				defer close(outputChannel)
				partitions := 3
				messagesPerPartition := 3
				messagesToSend := make([]*sarama.ConsumerMessage, 0, 3)
				for partition := 0; partition < partitions; partition++ {
					kafkaMessages := make(chan *sarama.ConsumerMessage, 10)
					for offset := 0; offset < messagesPerPartition; offset++ {
						msg := generateMessage("topic1", partition, offset)
						messagesToSend = append(messagesToSend, msg)
						kafkaMessages <- msg
					}
					go func() {
						inputChannel := kafkaMessages
						defer close(inputChannel)
						err := handler.ProcessMessages(context.Background(), inputChannel, sess, watermill.LogFields{})
						assert.NoError(t, err)
					}()
				}
				receivedMessages := make([]*message.Message, 0, 3)
				nackedMessage := <-outputChannel
				receivedMessages = append(receivedMessages, nackedMessage)
				nackedMessage.Nack()
				for i := 0; i < messagesPerPartition; i++ {
					for j := 0; j < partitions; j++ {
						receivedMessages = append(receivedMessages, <-outputChannel)
					}

					// we do not get more messages because the batch was not processed yet
					waitOrFail(t, 100*time.Millisecond, outputChannel)

					// we ACK the messages
					for _, msg := range receivedMessages {
						msg.Ack()
					}
				}

				assert.Len(t, receivedMessages, len(messagesToSend)+1)
				testSameMessagesAndLocalOrder(t, receivedMessages[1:], messagesToSend)
				if sess != nil {
					assert.Eventually(t, func() bool { return len(mock.Calls) == 9 }, 1*time.Second, 10*time.Millisecond)
					mock.AssertNumberOfCalls(t, "MarkMessage", 9)
					for currentMessage := 0; currentMessage < messagesPerPartition; currentMessage++ {
						for currentPartition := 0; currentPartition < partitions; currentPartition++ {
							mock.AssertCalled(t, "MarkMessage", messagesToSend[messagesPerPartition*currentPartition+currentMessage], "")
						}
					}
				}
			})
			t.Run("NACK wait period is applied", func(t *testing.T) {
				testConfig := newTestConfig(test.hasConsumerGroup, 3)
				testConfig.nackResendSleep = 500 * time.Millisecond
				sess, mock := consumerGroupSession(testConfig)
				closing, outputChannel, handler := testPartitionConcurrentConsumption(testConfig)
				defer close(closing)
				defer close(outputChannel)
				messagesToSend := make([]*sarama.ConsumerMessage, 0, 3)
				kafkaMessages := make(chan *sarama.ConsumerMessage, 10)
				defer close(kafkaMessages)
				msg := generateMessage("topic1", 0, 0)
				messagesToSend = append(messagesToSend, msg)
				kafkaMessages <- msg
				go func() {
					err := handler.ProcessMessages(context.Background(), kafkaMessages, sess, watermill.LogFields{})
					assert.NoError(t, err)
				}()
				receivedMessages := make([]*message.Message, 0, 3)
				nackedMessage := <-outputChannel
				receivedMessages = append(receivedMessages, nackedMessage)
				nackedMessage.Nack()

				// we do not get more messages because the batch was not processed yet
				waitOrFail(t, 400*time.Millisecond, outputChannel)

				message := <-outputChannel
				receivedMessages = append(receivedMessages, message)
				message.Ack()
				assert.Len(t, receivedMessages, len(messagesToSend)+1)
				testSameMessagesAndLocalOrder(t, receivedMessages[1:], messagesToSend)
				if sess != nil {
					assert.Eventually(t, func() bool { return len(mock.Calls) == 1 }, 1*time.Second, 10*time.Millisecond)
					mock.AssertNumberOfCalls(t, "MarkMessage", 1)
					mock.AssertCalled(t, "MarkMessage", messagesToSend[0], "")
				}
			})
		})
	}

	t.Run("closing without events", func(t *testing.T) {
		testConfig := newTestConfig(false, 3)
		testConfig.nackResendSleep = 500 * time.Millisecond
		closing, outputChannel, handler := testPartitionConcurrentConsumption(testConfig)
		defer close(outputChannel)
		messagesToSend := make([]*sarama.ConsumerMessage, 0, 3)
		kafkaMessages := make(chan *sarama.ConsumerMessage, 10)
		defer close(kafkaMessages)
		msg := generateMessage("topic1", 0, 0)
		messagesToSend = append(messagesToSend, msg)
		kafkaMessages <- msg
		go func() {
			err := handler.ProcessMessages(context.Background(), kafkaMessages, nil, watermill.LogFields{})
			assert.NoError(t, err)
		}()
		close(closing)
	})
}

func testPartitionConcurrentConsumption(
	testConfig testConfig,
) (chan<- struct{}, chan *message.Message, MessageHandler) {
	outputChannel := make(chan *message.Message)
	closing := make(chan struct{})
	handler := NewPartitionConcurrentMessageHandler(
		outputChannel,
		DefaultMarshaler{},
		watermill.NopLogger{},
		closing,
		testConfig.nackResendSleep,
	)
	return closing, outputChannel, handler
}
