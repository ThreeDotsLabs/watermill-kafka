package kafka

import (
	"context"
)

type contextKey int

const (
	_ contextKey = iota
	partitionContextKey
	partitionOffsetContextKey
)

func setPartitionToCtx(ctx context.Context, partition int32) context.Context {
	return context.WithValue(ctx, partitionContextKey, partition)
}

// MessagePartitionFromCtx returns kafka partition of consumed message
func MessagePartitionFromCtx(ctx context.Context) (int32, bool) {
	partition, ok := ctx.Value(partitionContextKey).(int32)
	return partition, ok
}

func setPartitionOffsetToCtx(ctx context.Context, offset int64) context.Context {
	return context.WithValue(ctx, partitionOffsetContextKey, offset)
}

// MessagePartitionFromCtx returns kafka partition offset of consumed message
func MessagePartitionOffsetFromCtx(ctx context.Context) (int64, bool) {
	offset, ok := ctx.Value(partitionOffsetContextKey).(int64)
	return offset, ok
}
