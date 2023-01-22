//go:build stress
// +build stress

package kafka_test

import (
	"runtime"
	"testing"

	"github.com/ThreeDotsLabs/watermill/pubsub/tests"
)

func init() {
	// Set GOMAXPROCS to double the number of CPUs
	runtime.GOMAXPROCS(runtime.GOMAXPROCS(0) * 2)
}

func TestPublishSubscribe_stress(t *testing.T) {
	tests.TestPubSubStressTest(
		t,
		tests.Features{
			ConsumerGroups:      true,
			ExactlyOnceDelivery: false,
			GuaranteedOrder:     false,
			Persistent:          true,
		},
		createPubSub,
		createPubSubWithConsumerGrup,
	)
}

func TestPublishSubscribe_ordered_stress(t *testing.T) {
	tests.TestPubSubStressTest(
		t,
		tests.Features{
			ConsumerGroups:      true,
			ExactlyOnceDelivery: false,
			GuaranteedOrder:     true,
			Persistent:          true,
		},
		createPartitionedPubSub,
		createPubSubWithConsumerGrup,
	)
}
