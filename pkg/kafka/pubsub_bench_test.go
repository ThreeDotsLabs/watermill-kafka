package kafka_test

import (
	"testing"

	"github.com/IBM/sarama"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-kafka/v3/pkg/kafka"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/tests"
)

func BenchmarkSubscriber(b *testing.B) {
	runBenchmark(b, kafka.Default)
}

func BenchmarkSubscriberBatch(b *testing.B) {
	runBenchmark(b, kafka.Batch)
}

func BenchmarkSubscriberPartitionConcurrent(b *testing.B) {
	runBenchmark(b, kafka.PartitionConcurrent)
}

func runBenchmark(b *testing.B, consumerModel kafka.ConsumerModel) {
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
				ConsumerModel:         consumerModel,
			},
			logger,
		)
		if err != nil {
			panic(err)
		}

		return publisher, subscriber
	})
}
