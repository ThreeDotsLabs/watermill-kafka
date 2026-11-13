package kafka_test

import (
	"context"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-kafka/v3/pkg/kafka"
	"github.com/ThreeDotsLabs/watermill/message"
)

type ctxType string

type legacyUnmarshaler struct{}

func (legacyUnmarshaler) Unmarshal(kafkaMsg *sarama.ConsumerMessage) (*message.Message, error) {
	msg := message.NewMessage(watermill.NewUUID(), kafkaMsg.Value)
	msg.Metadata.Set("legacy", "true")
	return msg, nil
}

type contextAwareUnmarshaler struct{}

func (contextAwareUnmarshaler) Unmarshal(kafkaMsg *sarama.ConsumerMessage) (*message.Message, error) {
	return message.NewMessage(watermill.NewUUID(), kafkaMsg.Value), nil
}

func (contextAwareUnmarshaler) UnmarshalWithContext(ctx context.Context, kafkaMsg *sarama.ConsumerMessage) (*message.Message, error) {
	msg := message.NewMessage(watermill.NewUUID(), kafkaMsg.Value)
	msg.Metadata.Set("context-aware", "true")
	ctx = context.WithValue(ctx, ctxType("int-key"), ctxType("int-value"))
	msg.SetContext(ctx)
	return msg, nil
}

func TestSubscriber_LegacyUnmarshaler(t *testing.T) {
	brokers := kafkaBrokers()
	topic := "test-legacy-" + watermill.NewShortUUID()

	publisher, err := kafka.NewPublisher(
		kafka.PublisherConfig{
			Brokers:   brokers,
			Marshaler: kafka.DefaultMarshaler{},
		},
		watermill.NewStdLogger(false, false),
	)
	require.NoError(t, err)
	defer publisher.Close()

	subscriber, err := kafka.NewSubscriber(
		kafka.SubscriberConfig{
			Brokers:       brokers,
			Unmarshaler:   legacyUnmarshaler{},
			ConsumerGroup: "test-legacy-group-" + watermill.NewShortUUID(),
		},
		watermill.NewStdLogger(false, false),
	)
	require.NoError(t, err)
	defer subscriber.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	ctx = context.WithValue(ctx, ctxType("ext-key"), ctxType("ext-value"))
	defer cancel()

	messages, err := subscriber.Subscribe(ctx, topic)
	require.NoError(t, err)

	testMsg := message.NewMessage(watermill.NewUUID(), []byte("test payload"))
	err = publisher.Publish(topic, testMsg)
	require.NoError(t, err)

	select {
	case msg := <-messages:
		assert.Equal(t, "true", msg.Metadata.Get("legacy"))
		assert.Equal(t, "test payload", string(msg.Payload))
		assert.Equal(t, ctxType("ext-value"), msg.Context().Value(ctxType("ext-key")))
		msg.Ack()
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for message")
	}
}

func TestSubscriber_ContextAwareUnmarshaler(t *testing.T) {
	brokers := kafkaBrokers()
	topic := "test-context-aware-" + watermill.NewShortUUID()

	publisher, err := kafka.NewPublisher(
		kafka.PublisherConfig{
			Brokers:   brokers,
			Marshaler: kafka.DefaultMarshaler{},
		},
		watermill.NewStdLogger(false, false),
	)
	require.NoError(t, err)
	defer publisher.Close()

	subscriber, err := kafka.NewSubscriber(
		kafka.SubscriberConfig{
			Brokers:       brokers,
			Unmarshaler:   contextAwareUnmarshaler{},
			ConsumerGroup: "test-context-group-" + watermill.NewShortUUID(),
		},
		watermill.NewStdLogger(false, false),
	)
	require.NoError(t, err)
	defer subscriber.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	ctx = context.WithValue(ctx, ctxType("ext-key"), ctxType("ext-value"))
	defer cancel()

	messages, err := subscriber.Subscribe(ctx, topic)
	require.NoError(t, err)

	// Publish a test message
	testMsg := message.NewMessage(watermill.NewUUID(), []byte("test payload"))
	err = publisher.Publish(topic, testMsg)
	require.NoError(t, err)

	// Receive and verify message
	select {
	case msg := <-messages:
		assert.Equal(t, "true", msg.Metadata.Get("context-aware"))
		assert.Equal(t, "test payload", string(msg.Payload))
		assert.Equal(t, ctxType("ext-value"), msg.Context().Value(ctxType("ext-key")))
		assert.Equal(t, ctxType("int-value"), msg.Context().Value(ctxType("int-key")))
		msg.Ack()
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for message")
	}
}
