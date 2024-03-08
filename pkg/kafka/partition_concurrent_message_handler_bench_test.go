package kafka

import (
	"testing"
)

func BenchmarkPartitionConcurrentMessageHandler(b *testing.B) {
	tests := []struct {
		name                     string
		hasConsumerGroup         bool
		hasCountingConsumerGroup bool
	}{
		{
			name:             "no consumer group session",
			hasConsumerGroup: false,
		},
		{
			name:                     "consumer group session is provided",
			hasConsumerGroup:         true,
			hasCountingConsumerGroup: false,
		},
	}
	for _, test := range tests {
		b.Run("consumes all messages in order", func(b *testing.B) {
			benchmarkPartitionConcurrentMessageHandler(b, test.hasConsumerGroup)
		})
	}
}

func benchmarkPartitionConcurrentMessageHandler(b *testing.B, hasConsumerGroup bool) {
	testConfig := testConfig{
		hasConsumerGroup:         hasConsumerGroup,
		hasCountingConsumerGroup: false,
	}

	testBenchmark(b, testConfig, testPartitionConcurrentConsumption)
}
