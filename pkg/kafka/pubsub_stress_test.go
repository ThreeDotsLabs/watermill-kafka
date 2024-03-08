//go:build stress
// +build stress

package kafka_test

import (
	"runtime"
	"testing"

	"github.com/ThreeDotsLabs/watermill-kafka/v2/pkg/kafka"
	"github.com/ThreeDotsLabs/watermill/pubsub/tests"
)

func init() {
	// Set GOMAXPROCS to double the number of CPUs
	runtime.GOMAXPROCS(runtime.GOMAXPROCS(0) * 2)
}

func TestPublishSubscribe_stress(t *testing.T) {
	t.Run("default consumption model", func(t *testing.T) {
		tests.TestPubSubStressTest(
			t,
			tests.Features{
				ConsumerGroups:      true,
				ExactlyOnceDelivery: false,
				GuaranteedOrder:     false,
				Persistent:          true,
			},
			createPubSub(kafka.Default),
			createPubSubWithConsumerGroup(kafka.Default),
		)
	})

	t.Run("batch consumption model", func(t *testing.T) {
		testBulkMessageHandlerPubSubStressTest(
			t,
			tests.Features{
				ConsumerGroups:      true,
				ExactlyOnceDelivery: false,
				GuaranteedOrder:     true,
				Persistent:          true,
			},
			createPartitionedPubSub(kafka.Batch),
			createPubSubWithConsumerGroup(kafka.Batch),
		)
	})

	t.Run("partition concurrent consumption model", func(t *testing.T) {
		tests.TestPubSubStressTest(
			t,
			tests.Features{
				ConsumerGroups:      true,
				ExactlyOnceDelivery: false,
				GuaranteedOrder:     false,
				Persistent:          true,
			},
			createPubSub(kafka.PartitionConcurrent),
			createPubSubWithConsumerGroup(kafka.PartitionConcurrent),
		)
	})
}

func TestPublishSubscribe_ordered_stress(t *testing.T) {
	t.Run("default consumption model", func(t *testing.T) {
		tests.TestPubSubStressTest(
			t,
			tests.Features{
				ConsumerGroups:      true,
				ExactlyOnceDelivery: false,
				GuaranteedOrder:     true,
				Persistent:          true,
			},
			createPartitionedPubSub(kafka.Default),
			createPubSubWithConsumerGroup(kafka.Default),
		)
	})

	t.Run("batch consumption model", func(t *testing.T) {
		testBulkMessageHandlerPubSubStressTest(
			t,
			tests.Features{
				ConsumerGroups:      true,
				ExactlyOnceDelivery: false,
				GuaranteedOrder:     true,
				Persistent:          true,
			},
			createPartitionedPubSub(kafka.Batch),
			createPubSubWithConsumerGroup(kafka.Batch),
		)
	})

	t.Run("partition concurrent consumption model", func(t *testing.T) {
		tests.TestPubSubStressTest(
			t,
			tests.Features{
				ConsumerGroups:      true,
				ExactlyOnceDelivery: false,
				GuaranteedOrder:     true,
				Persistent:          true,
			},
			createPartitionedPubSub(kafka.PartitionConcurrent),
			createPubSubWithConsumerGroup(kafka.PartitionConcurrent),
		)
	})
}
