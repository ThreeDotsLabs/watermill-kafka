package kafka_test

import (
	"testing"

	"github.com/Shopify/sarama"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-kafka/v2/pkg/kafka"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/tests"
)

func BenchmarkSubscriber(b *testing.B) {
	tests.BenchSubscriber(b, func(n int) (message.Publisher, message.Subscriber) {
		logger := watermill.NopLogger{}

		publisher, err := kafka.NewPublisher(kafka.PublisherConfig{
			Brokers:   kafkaBrokers(),
			Marshaler: kafka.DefaultMarshaler{},
		}, logger)
		if err != nil {
			panic(err)
		}

		saramaConfig := kafka.DefaultSaramaSubscriberConfig()
		saramaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest

		subscriber, err := kafka.NewSubscriber(
			kafka.SubscriberConfig{
				Brokers:               kafkaBrokers(),
				Unmarshaler:           kafka.DefaultMarshaler{},
				OverwriteSaramaConfig: saramaConfig,
				ConsumerGroup:         "test",
			},
			logger,
		)
		if err != nil {
			panic(err)
		}

		return publisher, subscriber
	})
}
