package kafka

import (
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/pkg/errors"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

type Publisher struct {
	config        PublisherConfig
	producer      sarama.SyncProducer
	asyncProducer sarama.AsyncProducer
	logger        watermill.LoggerAdapter

	asyncDrainWg sync.WaitGroup

	closed bool
}

// NewPublisher creates a new Kafka Publisher.
func NewPublisher(
	config PublisherConfig,
	logger watermill.LoggerAdapter,
) (*Publisher, error) {
	config.setDefaults()

	if err := config.Validate(); err != nil {
		return nil, err
	}

	if logger == nil {
		logger = watermill.NopLogger{}
	}

	if config.OTELEnabled && config.Tracer == nil {
		config.Tracer = NewOTELSaramaTracer()
	}

	if config.AsyncProducer {
		return newAsyncPublisher(config, logger)
	}

	producer, err := sarama.NewSyncProducer(config.Brokers, config.OverwriteSaramaConfig)
	if err != nil {
		return nil, errors.Wrap(err, "cannot create Kafka producer")
	}

	if config.Tracer != nil {
		producer = config.Tracer.WrapSyncProducer(config.OverwriteSaramaConfig, producer)
	}

	return &Publisher{
		config:   config,
		producer: producer,
		logger:   logger,
	}, nil
}

func newAsyncPublisher(config PublisherConfig, logger watermill.LoggerAdapter) (*Publisher, error) {
	producer, err := sarama.NewAsyncProducer(config.Brokers, config.OverwriteSaramaConfig)
	if err != nil {
		return nil, errors.Wrap(err, "cannot create Kafka async producer")
	}

	if config.Tracer != nil {
		if tracer, ok := config.Tracer.(SaramaAsyncTracer); ok {
			producer = tracer.WrapAsyncProducer(config.OverwriteSaramaConfig, producer)
		} else {
			logger.Info("Tracer does not implement SaramaAsyncTracer, async producer will not be traced", nil)
		}
	}

	p := &Publisher{
		config:        config,
		asyncProducer: producer,
		logger:        logger,
	}

	p.asyncDrainWg.Add(2)
	go func() {
		defer p.asyncDrainWg.Done()
		for producerErr := range producer.Errors() {
			if config.AsyncErrorHandler != nil {
				config.AsyncErrorHandler(producerErr)
				continue
			}

			logFields := watermill.LogFields{}
			if producerErr.Msg != nil {
				logFields["topic"] = producerErr.Msg.Topic
			}
			logger.Error("Cannot produce message", producerErr.Err, logFields)
		}
	}()
	go func() {
		defer p.asyncDrainWg.Done()
		for msg := range producer.Successes() {
			if config.AsyncSuccessHandler != nil {
				config.AsyncSuccessHandler(msg)
			}
		}
	}()

	return p, nil
}

type PublisherConfig struct {
	// Kafka brokers list.
	Brokers []string

	// Marshaler is used to marshal messages from Watermill format into Kafka format.
	Marshaler Marshaler

	// OverwriteSaramaConfig holds additional sarama settings.
	OverwriteSaramaConfig *sarama.Config

	// If true then each sent message will be wrapped with Opentelemetry tracing, provided by otelsarama.
	OTELEnabled bool

	// Tracer is used to trace Kafka messages.
	// If nil, then no tracing will be used.
	Tracer SaramaTracer

	// AsyncProducer, if true, uses sarama.AsyncProducer under the hood.
	// Publish then enqueues messages without waiting for the broker's ack, which
	// significantly increases throughput at the cost of weaker delivery guarantees:
	// delivery errors are not returned from Publish, but passed to AsyncErrorHandler
	// (or logged, if no handler is set). Close flushes all enqueued messages.
	AsyncProducer bool

	// AsyncErrorHandler is called for every message that failed to be delivered
	// when AsyncProducer is enabled. If nil, failures are logged instead.
	// It requires Producer.Return.Errors set to true in OverwriteSaramaConfig
	// (it is true in DefaultSaramaAsyncPublisherConfig).
	// The handler must not block: it is called from the goroutine that drains
	// the producer's errors channel. Ignored when AsyncProducer is false.
	AsyncErrorHandler func(err *sarama.ProducerError)

	// AsyncSuccessHandler is called for every message acked by the broker when
	// AsyncProducer is enabled. It requires Producer.Return.Successes set to true
	// in OverwriteSaramaConfig (it is false in DefaultSaramaAsyncPublisherConfig).
	// The handler must not block: it is called from the goroutine that drains
	// the producer's successes channel. Ignored when AsyncProducer is false.
	AsyncSuccessHandler func(msg *sarama.ProducerMessage)
}

func (c *PublisherConfig) setDefaults() {
	if c.OverwriteSaramaConfig == nil {
		if c.AsyncProducer {
			c.OverwriteSaramaConfig = DefaultSaramaAsyncPublisherConfig()
		} else {
			c.OverwriteSaramaConfig = DefaultSaramaSyncPublisherConfig()
		}
	}
	if c.Marshaler == nil {
		c.Marshaler = DefaultMarshaler{}
	}
}

func (c PublisherConfig) Validate() error {
	if len(c.Brokers) == 0 {
		return errors.New("missing brokers")
	}
	if c.Marshaler == nil {
		return errors.New("missing marshaler")
	}

	return nil
}

func DefaultSaramaSyncPublisherConfig() *sarama.Config {
	config := sarama.NewConfig()

	config.Producer.Retry.Max = 10
	config.Producer.Return.Successes = true
	config.Metadata.Retry.Backoff = time.Second * 2
	config.ClientID = "watermill"

	return config
}

func DefaultSaramaAsyncPublisherConfig() *sarama.Config {
	config := DefaultSaramaSyncPublisherConfig()

	// Draining the successes channel for every message defeats the purpose of
	// an async producer; delivery failures are still reported via the errors channel.
	config.Producer.Return.Successes = false
	config.Producer.Return.Errors = true

	return config
}

// Publish publishes message to Kafka.
//
// Publish is blocking and wait for ack from Kafka.
// When one of messages delivery fails - function is interrupted.
//
// If PublisherConfig.AsyncProducer is enabled, Publish only enqueues messages
// and returns without waiting for acks; delivery errors are passed to
// PublisherConfig.AsyncErrorHandler or logged.
func (p *Publisher) Publish(topic string, msgs ...*message.Message) error {
	if p.closed {
		return errors.New("publisher closed")
	}

	logFields := make(watermill.LogFields, 4)
	logFields["topic"] = topic

	for _, msg := range msgs {
		logFields["message_uuid"] = msg.UUID
		p.logger.Trace("Sending message to Kafka", logFields)

		kafkaMsg, err := p.config.Marshaler.Marshal(topic, msg)
		if err != nil {
			return errors.Wrapf(err, "cannot marshal message %s", msg.UUID)
		}

		if p.config.AsyncProducer {
			p.asyncProducer.Input() <- kafkaMsg
			p.logger.Trace("Message enqueued for Kafka", logFields)
			continue
		}

		partition, offset, err := p.producer.SendMessage(kafkaMsg)
		if err != nil {
			return errors.Wrapf(err, "cannot produce message %s", msg.UUID)
		}

		logFields["kafka_partition"] = partition
		logFields["kafka_partition_offset"] = offset

		p.logger.Trace("Message sent to Kafka", logFields)
	}

	return nil
}

func (p *Publisher) Close() error {
	if p.closed {
		return nil
	}
	p.closed = true

	if p.asyncProducer != nil {
		// AsyncClose flushes all buffered messages and then closes the successes and
		// errors channels, so the draining goroutines handle every delivery failure
		// before the wait returns. sarama's blocking Close is not used because it
		// would compete with the draining goroutines for the errors channel.
		p.asyncProducer.AsyncClose()
		p.asyncDrainWg.Wait()

		return nil
	}

	if err := p.producer.Close(); err != nil {
		return errors.Wrap(err, "cannot close Kafka producer")
	}

	return nil
}
