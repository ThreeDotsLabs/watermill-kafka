package kafka

import (
	"context"
)

type contextKey int

const (
	_ contextKey = iota
	topicContextKey
	partitionContextKey
	partitionOffsetContextKey
)

func setTopicToCtx(ctx context.Context, topic string) context.Context {
	return context.WithValue(ctx, topicContextKey, topic)
}

// MessageTopicFromCtx returns kafka topic of consumed message
func MessageTopicFromCtx(ctx context.Context) string {
	topic := ctx.Value(topicContextKey)

	if stringTopic, ok := topic.(string); ok {
		return stringTopic
	} else {
		return ""
	}
}

func setPartitionToCtx(ctx context.Context, partition int32) context.Context {
	return context.WithValue(ctx, partitionContextKey, partition)
}

// MessagePartitionFromCtx returns kafka partition of consumed message
func MessagePartitionFromCtx(ctx context.Context) int32 {
	partition := ctx.Value(partitionContextKey)

	if intPartition, ok := partition.(int32); ok {
		return intPartition
	} else {
		return 0
	}
}

func setPartitionOffsetToCtx(ctx context.Context, offset int64) context.Context {
	return context.WithValue(ctx, partitionOffsetContextKey, offset)
}

// MessagePartitionFromCtx returns kafka partition offset of consumed message
func MessagePartitionOffsetFromCtx(ctx context.Context) int64 {
	partitionOffset := ctx.Value(partitionOffsetContextKey)

	if intPartitionOffset, ok := partitionOffset.(int64); ok {
		return intPartitionOffset
	} else {
		return 0
	}
}
