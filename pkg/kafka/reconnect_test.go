//go:build reconnect

package kafka_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/require"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-kafka/v3/pkg/kafka"
)

// reconnectCountingLogger counts how many times "Reconnecting consumer" is logged.
// handleReconnects emits that line once per reconnect cycle, so the count is a
// direct measure of reconnect churn.
type reconnectCountingLogger struct {
	reconnects int64
}

func (l *reconnectCountingLogger) Error(msg string, _ error, _ watermill.LogFields) {
	l.maybeCount(msg)
}

func (l *reconnectCountingLogger) Info(msg string, _ watermill.LogFields) {
	l.maybeCount(msg)
}

func (l *reconnectCountingLogger) Debug(string, watermill.LogFields) {}

func (l *reconnectCountingLogger) Trace(string, watermill.LogFields) {}

func (l *reconnectCountingLogger) With(watermill.LogFields) watermill.LoggerAdapter {
	return l
}

func (l *reconnectCountingLogger) maybeCount(msg string) {
	if msg == "Reconnecting consumer" {
		atomic.AddInt64(&l.reconnects, 1)
	}
}

func (l *reconnectCountingLogger) count() int64 {
	return atomic.LoadInt64(&l.reconnects)
}

// TestReconnect_AsyncConsumeError_RespectsReconnectRetrySleep verifies that a
// repeatedly-failing group.Consume backs off ReconnectRetrySleep between reconnects
// instead of spinning a fresh client and consumer group per cycle. It subscribes to a
// missing topic with auto-create disabled, so NewConsumerGroupFromClient succeeds and
// consumeMessages returns nil, but group.Consume errors at once. Unpatched, the sleep
// sits in the err != nil branch this async path never reaches, so reconnects spin; with
// the fix the count stays bounded by window/ReconnectRetrySleep.
func TestReconnect_AsyncConsumeError_RespectsReconnectRetrySleep(t *testing.T) {
	const (
		reconnectRetrySleep = 200 * time.Millisecond
		observationWindow   = 3 * time.Second
		// window/sleep == 15 reconnects in the ideal case; allow generous slack for
		// scheduling and the in-flight cycle. Unpatched produces hundreds/thousands.
		maxReconnects = 25
	)

	brokers := kafkaBrokers()
	topic := fmt.Sprintf("reconnect_async_error_nonexistent_%s", watermill.NewShortUUID())

	saramaConfig := kafka.DefaultSaramaSubscriberConfig()
	saramaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
	saramaConfig.Consumer.Group.Heartbeat.Interval = 500 * time.Millisecond
	saramaConfig.Consumer.Group.Rebalance.Timeout = 3 * time.Second
	// Keep the broker from auto-creating the topic so group.Consume keeps failing.
	saramaConfig.Metadata.AllowAutoTopicCreation = false
	// Make each failing reconnect cycle cheap (no metadata retry/backoff, no full
	// refresh) so the unthrottled churn is plainly visible within the window. Without
	// this the per-cycle metadata round-trips act as an implicit backoff and mask the
	// missing ReconnectRetrySleep.
	saramaConfig.Metadata.Retry.Max = 0
	saramaConfig.Metadata.Retry.Backoff = 0
	saramaConfig.Metadata.Full = false

	logger := &reconnectCountingLogger{}

	sub, err := kafka.NewSubscriber(
		kafka.SubscriberConfig{
			Brokers:               brokers,
			Unmarshaler:           kafka.DefaultMarshaler{},
			OverwriteSaramaConfig: saramaConfig,
			ConsumerGroup:         "reconnect_async_error_group",
			ReconnectRetrySleep:   reconnectRetrySleep,
		},
		logger,
	)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	messages, err := sub.Subscribe(ctx, topic)
	require.NoError(t, err)

	// Drain the output channel so Subscribe's goroutine is never blocked on delivery.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range messages {
		}
	}()

	// Let the consumer group establish and start failing, then measure the reconnect
	// rate over a fixed window.
	time.Sleep(time.Second)

	before := logger.count()
	time.Sleep(observationWindow)
	reconnectsInWindow := logger.count() - before

	require.NoError(t, sub.Close())
	cancel()
	wg.Wait()

	t.Logf("reconnects observed in %s window (ReconnectRetrySleep=%s): %d",
		observationWindow, reconnectRetrySleep, reconnectsInWindow)

	require.LessOrEqualf(t, reconnectsInWindow, int64(maxReconnects),
		"reconnect churn not throttled: observed %d reconnects in %s with ReconnectRetrySleep=%s (expected <= %d)",
		reconnectsInWindow, observationWindow, reconnectRetrySleep, maxReconnects)
}
