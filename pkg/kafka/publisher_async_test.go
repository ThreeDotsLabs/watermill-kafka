package kafka_test

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-kafka/v3/pkg/kafka"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/subscriber"
)

func newAsyncPublisher(t *testing.T, overwriteConfig func(*kafka.PublisherConfig)) *kafka.Publisher {
	config := kafka.PublisherConfig{
		Brokers:       kafkaBrokers(),
		Marshaler:     kafka.DefaultMarshaler{},
		AsyncProducer: true,
	}
	if overwriteConfig != nil {
		overwriteConfig(&config)
	}

	var err error
	var publisher *kafka.Publisher

	retriesLeft := 5
	for {
		publisher, err = kafka.NewPublisher(config, watermill.NewStdLogger(false, false))
		if err == nil || retriesLeft == 0 {
			break
		}

		retriesLeft--
		fmt.Printf("cannot create kafka Publisher: %s, retrying (%d retries left)", err, retriesLeft)
		time.Sleep(time.Second * 2)
	}
	require.NoError(t, err)

	return publisher
}

func TestPublishSubscribe_async(t *testing.T) {
	pub := newAsyncPublisher(t, nil)
	syncPub, sub := newPubSub(t, kafka.DefaultMarshaler{}, "")
	require.NoError(t, syncPub.Close())
	topicName := "topic_" + watermill.NewUUID()

	var messagesToPublish []*message.Message
	for i := 0; i < 100; i++ {
		messagesToPublish = append(messagesToPublish, message.NewMessage(watermill.NewUUID(), nil))
	}

	err := pub.Publish(topicName, messagesToPublish...)
	require.NoError(t, err, "cannot publish messages")

	// Close must flush all enqueued messages before returning.
	require.NoError(t, pub.Close())

	messages, err := sub.Subscribe(context.Background(), topicName)
	require.NoError(t, err)

	receivedMessages, all := subscriber.BulkReadWithDeduplication(messages, len(messagesToPublish), time.Second*30)
	assert.True(t, all)
	assert.Len(t, receivedMessages, len(messagesToPublish))

	require.NoError(t, sub.Close())
}

func TestPublishSubscribe_async_error_handler(t *testing.T) {
	producerErrs := make(chan *sarama.ProducerError, 1)

	saramaConfig := kafka.DefaultSaramaAsyncPublisherConfig()
	// Force a delivery failure: the message below exceeds the limit.
	saramaConfig.Producer.MaxMessageBytes = 64

	pub := newAsyncPublisher(t, func(config *kafka.PublisherConfig) {
		config.OverwriteSaramaConfig = saramaConfig
		config.AsyncErrorHandler = func(err *sarama.ProducerError) {
			select {
			case producerErrs <- err:
			default:
			}
		}
	})
	topicName := "topic_" + watermill.NewUUID()

	msg := message.NewMessage(watermill.NewUUID(), bytes.Repeat([]byte("x"), 1024))
	require.NoError(t, pub.Publish(topicName, msg))

	select {
	case producerErr := <-producerErrs:
		require.NotNil(t, producerErr.Msg)
		assert.Equal(t, topicName, producerErr.Msg.Topic)
		assert.Error(t, producerErr.Err)
	case <-time.After(time.Second * 10):
		t.Fatal("expected AsyncErrorHandler to be called")
	}

	require.NoError(t, pub.Close())
}

func TestPublishSubscribe_async_success_handler(t *testing.T) {
	successes := make(chan *sarama.ProducerMessage, 1)

	saramaConfig := kafka.DefaultSaramaAsyncPublisherConfig()
	saramaConfig.Producer.Return.Successes = true

	pub := newAsyncPublisher(t, func(config *kafka.PublisherConfig) {
		config.OverwriteSaramaConfig = saramaConfig
		config.AsyncSuccessHandler = func(msg *sarama.ProducerMessage) {
			select {
			case successes <- msg:
			default:
			}
		}
	})
	topicName := "topic_" + watermill.NewUUID()

	msg := message.NewMessage(watermill.NewUUID(), []byte("payload"))
	require.NoError(t, pub.Publish(topicName, msg))

	select {
	case producerMsg := <-successes:
		assert.Equal(t, topicName, producerMsg.Topic)
	case <-time.After(time.Second * 10):
		t.Fatal("expected AsyncSuccessHandler to be called")
	}

	require.NoError(t, pub.Close())
}
