//go:build stress
// +build stress

package kafka_test

import (
	"runtime"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill-kafka/v2/pkg/kafka"
	"github.com/ThreeDotsLabs/watermill/pubsub/tests"
)

func init() {
	// Set GOMAXPROCS to double the number of CPUs
	runtime.GOMAXPROCS(runtime.GOMAXPROCS(0) * 2)
}

func TestPublishSubscribe_stress(t *testing.T) {
	t.Run("no batch consumer config", func(t *testing.T) {
		tests.TestPubSubStressTest(
			t,
			tests.Features{
				ConsumerGroups:      true,
				ExactlyOnceDelivery: false,
				GuaranteedOrder:     false,
				Persistent:          true,
			},
			createPubSub(nil),
			createPubSubWithConsumerGroup(nil),
		)
	})

	t.Run("with batch consumer config", func(t *testing.T) {
		cfg := &kafka.BatchConsumerConfig{
			MaxBatchSize: 10,
			MaxWaitTime:  100 * time.Millisecond,
		}
		testBulkMessageHandlerPubSubStressTest(
			t,
			tests.Features{
				ConsumerGroups:      true,
				ExactlyOnceDelivery: false,
				GuaranteedOrder:     true,
				Persistent:          true,
			},
			createPartitionedPubSub(cfg),
			createPubSubWithConsumerGroup(cfg),
		)
	})
}

func TestPublishSubscribe_ordered_stress(t *testing.T) {
	t.Run("no batch consumer config", func(t *testing.T) {
		tests.TestPubSubStressTest(
			t,
			tests.Features{
				ConsumerGroups:      true,
				ExactlyOnceDelivery: false,
				GuaranteedOrder:     true,
				Persistent:          true,
			},
			createPartitionedPubSub(nil),
			createPubSubWithConsumerGroup(nil),
		)
	})

	t.Run("with batch consumer config", func(t *testing.T) {
		cfg := &kafka.BatchConsumerConfig{
			MaxBatchSize: 10,
			MaxWaitTime:  100 * time.Millisecond,
		}
		testBulkMessageHandlerPubSubStressTest(
			t,
			tests.Features{
				ConsumerGroups:      true,
				ExactlyOnceDelivery: false,
				GuaranteedOrder:     true,
				Persistent:          true,
			},
			createPartitionedPubSub(cfg),
			createPubSubWithConsumerGroup(cfg),
		)
	})
}
