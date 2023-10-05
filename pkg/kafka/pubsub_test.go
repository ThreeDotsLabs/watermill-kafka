package kafka_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-kafka/v3/pkg/kafka"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/subscriber"
	"github.com/ThreeDotsLabs/watermill/pubsub/tests"
)

func kafkaBrokers() []string {
	brokers := os.Getenv("WATERMILL_TEST_KAFKA_BROKERS")
	if brokers != "" {
		return strings.Split(brokers, ",")
	}
	return []string{"localhost:9091", "localhost:9092", "localhost:9093", "localhost:9094", "localhost:9095"}
}

func newPubSub(t *testing.T, marshaler kafka.MarshalerUnmarshaler, consumerGroup string, consumerModel kafka.ConsumerModel) (*kafka.Publisher, *kafka.Subscriber) {
	logger := watermill.NewStdLogger(true, true)

	var err error
	var publisher *kafka.Publisher

	retriesLeft := 5
	for {
		publisher, err = kafka.NewPublisher(kafka.PublisherConfig{
			Brokers:   kafkaBrokers(),
			Marshaler: marshaler,
		}, logger)
		if err == nil || retriesLeft == 0 {
			break
		}

		retriesLeft--
		fmt.Printf("cannot create kafka Publisher: %s, retrying (%d retries left)", err, retriesLeft)
		time.Sleep(time.Second * 2)
	}
	require.NoError(t, err)

	saramaConfig := kafka.DefaultSaramaSubscriberConfig()
	saramaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest

	saramaConfig.Admin.Timeout = time.Second * 30
	saramaConfig.Producer.RequiredAcks = sarama.WaitForAll
	saramaConfig.ChannelBufferSize = 10240
	saramaConfig.Consumer.Group.Heartbeat.Interval = time.Millisecond * 500
	saramaConfig.Consumer.Group.Rebalance.Timeout = time.Second * 3

	var subscriber *kafka.Subscriber

	retriesLeft = 5
	for {
		subscriber, err = kafka.NewSubscriber(
			kafka.SubscriberConfig{
				Brokers:               kafkaBrokers(),
				Unmarshaler:           marshaler,
				OverwriteSaramaConfig: saramaConfig,
				ConsumerGroup:         consumerGroup,
				InitializeTopicDetails: &sarama.TopicDetail{
					NumPartitions:     8,
					ReplicationFactor: 1,
				},
				BatchConsumerConfig: &kafka.BatchConsumerConfig{
					MaxBatchSize: 10,
					MaxWaitTime:  100 * time.Millisecond,
				},
			},
			logger,
		)
		if err == nil || retriesLeft == 0 {
			break
		}

		retriesLeft--
		fmt.Printf("cannot create kafka Subscriber: %s, retrying (%d retries left)", err, retriesLeft)
		time.Sleep(time.Second * 2)
	}

	require.NoError(t, err)

	return publisher, subscriber
}

func generatePartitionKey(topic string, msg *message.Message) (string, error) {
	return msg.Metadata.Get("partition_key"), nil
}

func createPubSubWithConsumerGroup(consumerModel kafka.ConsumerModel) func(t *testing.T, consumerGroup string) (message.Publisher, message.Subscriber) {
	return func(t *testing.T, consumerGroup string) (message.Publisher, message.Subscriber) {
		return newPubSub(t, kafka.DefaultMarshaler{}, consumerGroup, consumerModel)
	}
}

func createPubSub(consumerModel kafka.ConsumerModel) func(t *testing.T) (message.Publisher, message.Subscriber) {
	return func(t *testing.T) (message.Publisher, message.Subscriber) {
		return createPubSubWithConsumerGroup(consumerModel)(t, "test")
	}
}

func createPartitionedPubSub(consumerModel kafka.ConsumerModel) func(*testing.T) (message.Publisher, message.Subscriber) {
	return func(t *testing.T) (message.Publisher, message.Subscriber) {
		return newPubSub(t, kafka.NewWithPartitioningMarshaler(generatePartitionKey), "test", consumerModel)
	}
}

func createNoGroupPubSub(consumerModel kafka.ConsumerModel) func(t *testing.T) (message.Publisher, message.Subscriber) {
	return func(t *testing.T) (message.Publisher, message.Subscriber) {
		return newPubSub(t, kafka.DefaultMarshaler{}, "", consumerModel)
	}
}

func TestPublishSubscribe(t *testing.T) {
	testCases := []struct {
		name          string
		consumerModel kafka.ConsumerModel
	}{
		{
			name:          "no batch consumer config",
			consumerModel: kafka.Default,
		},
		{
			name:          "with batch config",
			consumerModel: kafka.Batch,
		},
		{
			name:          "with concurrent config",
			consumerModel: kafka.PartitionConcurrent,
		},
	}
	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			features := tests.Features{
				ConsumerGroups:      true,
				ExactlyOnceDelivery: false,
				GuaranteedOrder:     false,
				Persistent:          true,
			}

			tests.TestPubSub(
				t,
				features,
				createPubSub(test.consumerModel),
				createPubSubWithConsumerGroup(test.consumerModel),
			)
		})
	}
}

func TestPublishSubscribe_ordered(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping long tests")
	}

	testCases := []struct {
		name          string
		consumerModel kafka.ConsumerModel
	}{
		{
			name:          "no batch consumer config",
			consumerModel: kafka.Default,
		},
		{
			name:          "with concurrent config",
			consumerModel: kafka.PartitionConcurrent,
		},
	}
	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			tests.TestPubSub(
				t,
				tests.Features{
					ConsumerGroups:      true,
					ExactlyOnceDelivery: false,
					GuaranteedOrder:     true,
					Persistent:          true,
				},
				createPartitionedPubSub(test.consumerModel),
				createPubSubWithConsumerGroup(test.consumerModel),
			)
		})
	}

	t.Run("with batch consumer config", func(t *testing.T) {
		testBulkMessageHandlerPubSub(
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
}

func TestNoGroupSubscriber(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping long tests")
	}
	testCases := []struct {
		name          string
		consumerModel kafka.ConsumerModel
	}{
		{
			name:          "no batch consumer config",
			consumerModel: kafka.Default,
		},
		{
			name:          "with batch config",
			consumerModel: kafka.Batch,
		},
		{
			name:          "with concurrent config",
			consumerModel: kafka.PartitionConcurrent,
		},
	}
	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			tests.TestPubSub(
				t,
				tests.Features{
					ConsumerGroups:                   false,
					ExactlyOnceDelivery:              false,
					GuaranteedOrder:                  false,
					Persistent:                       true,
					NewSubscriberReceivesOldMessages: true,
				},
				createNoGroupPubSub(test.consumerModel),
				nil,
			)
		})
	}
}

func TestCtxValues(t *testing.T) {
	tests := []struct {
		name          string
		consumerModel kafka.ConsumerModel
	}{
		{
			name:          "no batch consumer config",
			consumerModel: kafka.Default,
		},
		{
			name:          "with batch config",
			consumerModel: kafka.Batch,
		},
		{
			name:          "with concurrent config",
			consumerModel: kafka.PartitionConcurrent,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			pub, sub := newPubSub(t, kafka.DefaultMarshaler{}, "", test.consumerModel)
			topicName := "topic_" + watermill.NewUUID()

			var messagesToPublish []*message.Message

			for i := 0; i < 20; i++ {
				id := watermill.NewUUID()
				messagesToPublish = append(messagesToPublish, message.NewMessage(id, nil))
			}
			err := pub.Publish(topicName, messagesToPublish...)
			require.NoError(t, err, "cannot publish message")

			messages, err := sub.Subscribe(context.Background(), topicName)
			require.NoError(t, err)

			receivedMessages, all := subscriber.BulkReadWithDeduplication(messages, len(messagesToPublish), time.Second*10)
			require.True(t, all)

			expectedPartitionsOffsets := map[int32]int64{}
			for _, msg := range receivedMessages {
				partition, ok := kafka.MessagePartitionFromCtx(msg.Context())
				assert.True(t, ok)

				messagePartitionOffset, ok := kafka.MessagePartitionOffsetFromCtx(msg.Context())
				assert.True(t, ok)

				kafkaMsgTimestamp, ok := kafka.MessageTimestampFromCtx(msg.Context())
				assert.True(t, ok)
				assert.NotZero(t, kafkaMsgTimestamp)

				_, ok = kafka.MessageKeyFromCtx(msg.Context())
				assert.True(t, ok)

				if expectedPartitionsOffsets[partition] <= messagePartitionOffset {
					// kafka partition offset is offset of the last message + 1
					expectedPartitionsOffsets[partition] = messagePartitionOffset + 1
				}
			}
			assert.NotEmpty(t, expectedPartitionsOffsets)

			offsets, err := sub.PartitionOffset(topicName)
			require.NoError(t, err)
			assert.NotEmpty(t, offsets)

			assert.EqualValues(t, expectedPartitionsOffsets, offsets)

			require.NoError(t, pub.Close())
		})
	}
}
