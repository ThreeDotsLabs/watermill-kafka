package kafka

import (
	"go.opentelemetry.io/contrib/instrumentation/github.com/Shopify/sarama/otelsarama"
	"time"

	"github.com/Shopify/sarama"
	"github.com/pkg/errors"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

type PublisherAsync struct {
	config        PublisherConfig
	producer      sarama.AsyncProducer
	logger        watermill.LoggerAdapter
	errorsChan    <-chan *sarama.ProducerError
	successesChan <-chan *sarama.ProducerMessage

	closed bool
}

// NewAsyncPublisher creates a new Kafka PublisherAsync.
func NewAsyncPublisher(
	config PublisherConfig,
	logger watermill.LoggerAdapter,
) (*PublisherAsync, error) {
	config.setDefaults()

	if err := config.Validate(); err != nil {
		return nil, err
	}

	if logger == nil {
		logger = watermill.NopLogger{}
	}

	producer, err := sarama.NewAsyncProducer(config.Brokers, config.OverwriteSaramaConfig)
	if err != nil {
		return nil, errors.Wrap(err, "cannot create Kafka producer")
	}

	if config.OTELEnabled {
		producer = otelsarama.WrapAsyncProducer(config.OverwriteSaramaConfig, producer)
	}

	return &PublisherAsync{
		config:        config,
		producer:      producer,
		logger:        logger,
		errorsChan:    producer.Errors(),
		successesChan: producer.Successes(),
	}, nil
}

type PublisherAsyncConfig struct {
	// Kafka brokers list.
	Brokers []string

	// Marshaler is used to marshal messages from Watermill format into Kafka format.
	Marshaler Marshaler

	// OverwriteSaramaConfig holds additional sarama settings.
	OverwriteSaramaConfig *sarama.Config

	// If true then each sent message will be wrapped with Opentelemetry tracing, provided by otelsarama.
	OTELEnabled bool
}

func (c *PublisherAsyncConfig) setDefaults() {
	if c.OverwriteSaramaConfig == nil {
		c.OverwriteSaramaConfig = DefaultSaramaAsyncPublisherConfig()
	}
}

func (c PublisherAsyncConfig) Validate() error {
	if len(c.Brokers) == 0 {
		return errors.New("missing brokers")
	}
	if c.Marshaler == nil {
		return errors.New("missing marshaler")
	}

	return nil
}

func DefaultSaramaAsyncPublisherConfig() *sarama.Config {
	config := sarama.NewConfig()

	config.Producer.Retry.Max = 10
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Version = sarama.V1_0_0_0
	config.Metadata.Retry.Backoff = time.Second * 2
	config.ClientID = "watermill"

	return config
}

// Publish publishes message to Kafka.
//
// Publish is blocking and wait for ack from Kafka.
// When one of messages delivery fails - function is interrupted.
func (p *PublisherAsync) Publish(topic string, msgs ...*message.Message) error {
	if p.closed {
		return errors.New("publisher closed")
	}

	logFields := make(watermill.LogFields, 2)
	logFields["topic"] = topic

	for _, msg := range msgs {
		logFields["message_uuid"] = msg.UUID
		p.logger.Trace("Sending message to Kafka", logFields)

		kafkaMsg, err := p.config.Marshaler.Marshal(topic, msg)
		if err != nil {
			return errors.Wrapf(err, "cannot marshal message %s", msg.UUID)
		}

		p.producer.Input() <- kafkaMsg
	}

	return nil
}

func (p *PublisherAsync) Close() error {
	if p.closed {
		return nil
	}
	p.closed = true

	if err := p.producer.Close(); err != nil {
		return errors.Wrap(err, "cannot close Kafka producer")
	}

	return nil
}
