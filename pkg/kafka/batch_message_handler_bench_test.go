package kafka

import (
	"context"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func BenchmarkBatchMessageHandler(b *testing.B) {
	b.Run("consumer group session is provided", func(b *testing.B) {
		benchmarkBatchMessageHandler(b, true)
	})

	b.Run("no consumer group session", func(b *testing.B) {
		benchmarkBatchMessageHandler(b, true)
	})
}

func benchmarkBatchMessageHandler(b *testing.B, hasConsumerGroup bool) {
	testConfig := testConfig{
		batchWait:                100 * time.Millisecond,
		maxBatchSize:             10,
		hasConsumerGroup:         hasConsumerGroup,
		hasCountingConsumerGroup: false,
	}

	testBenchmark(b, testConfig, testBatchEventConsumption)
}

func testBenchmark(
	b *testing.B,
	testConfig testConfig,
	buildHandler func(testConfig) (chan<- struct{}, chan *message.Message, MessageHandler),
) {
	messagesInTest := 10_000
	sess, _ := consumerGroupSession(testConfig)
	kafkaMessages := make(chan *sarama.ConsumerMessage, messagesInTest)
	messagesToSend := make([]*sarama.ConsumerMessage, 0, messagesInTest)
	for i := 0; i < messagesInTest; i++ {
		msg := generateMessage("topic1", i%5, i/5)
		messagesToSend = append(messagesToSend, msg)
		kafkaMessages <- msg
	}
	closing, outputChannel, handler := buildHandler(
		testConfig,
	)
	defer close(closing)
	defer close(outputChannel)
	go func() {
		err := handler.ProcessMessages(context.Background(), kafkaMessages, sess, watermill.LogFields{})
		assert.NoError(b, err)
	}()
	receivedMessages := make([]*message.Message, 0, messagesInTest)
	for {
		select {
		case msg, ok := <-outputChannel:
			require.True(b, ok, "channel closed earlier than expected")
			receivedMessages = append(receivedMessages, msg)
			msg.Ack()
		}
		if len(receivedMessages) == messagesInTest {
			break
		}
	}
	testSameEventsAndSameLocalOrder(b, receivedMessages, messagesToSend)
}
