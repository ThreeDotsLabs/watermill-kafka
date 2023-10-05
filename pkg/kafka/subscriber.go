package kafka

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

type Subscriber struct {
	config SubscriberConfig
	logger watermill.LoggerAdapter

	closing       chan struct{}
	subscribersWg sync.WaitGroup

	closed bool
}

// NewSubscriber creates a new Kafka Subscriber.
func NewSubscriber(
	config SubscriberConfig,
	logger watermill.LoggerAdapter,
) (*Subscriber, error) {
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

	logger = logger.With(watermill.LogFields{
		"subscriber_uuid": watermill.NewShortUUID(),
	})

	return &Subscriber{
		config: config,
		logger: logger,

		closing: make(chan struct{}),
	}, nil
}

type SubscriberConfig struct {
	// Kafka brokers list.
	Brokers []string

	// Unmarshaler is used to unmarshal messages from Kafka format into Watermill format.
	Unmarshaler Unmarshaler

	// OverwriteSaramaConfig holds additional sarama settings.
	OverwriteSaramaConfig *sarama.Config

	// Kafka consumer group.
	// When empty, all messages from all partitions will be returned.
	ConsumerGroup string

	// How long after Nack message should be redelivered.
	NackResendSleep time.Duration

	// How long about unsuccessful reconnecting next reconnect will occur.
	ReconnectRetrySleep time.Duration

	InitializeTopicDetails *sarama.TopicDetail

	// If true then each consumed message will be wrapped with Opentelemetry tracing, provided by otelsarama.
	//
	// Deprecated: pass OTELSaramaTracer to Tracer field instead.
	OTELEnabled bool

	// Tracer is used to trace Kafka messages.
	// If nil, then no tracing will be used.
	Tracer SaramaTracer

	// ConsumerModel indicates which type of consumer should be used
	ConsumerModel ConsumerModel
	// When set to not nil, consumption will be performed in batches and this configuration will be used
	BatchConsumerConfig *BatchConsumerConfig
}

// BatchConsumerConfig configuration to be applied when the selected type of
// consumption is batch.
// Batch consumption means that the maxBatchSizeEvents will be read or maxWaitTime waited
// the events will then be sent to the output channel.
// ACK / NACK are handled properly to ensure at-least-once consumption.
type BatchConsumerConfig struct {
	// MaxBatchSize max amount of elements the batch will contain.
	// Default value is 100 if nothing is specified.
	MaxBatchSize int16
	// MaxWaitTime max time that it will be waited until MaxBatchSize elements are received.
	// Default value is 100ms if nothing is specified.
	MaxWaitTime time.Duration
}

// ConsumerModel indicates the type of consumer model that will be used.
type ConsumerModel int

const (
	// Default is a model when only one message is sent to the customer and customer needs to ACK the message
	// to receive the next.
	Default ConsumerModel = iota
	// Batch works by sending multiple events in a batch
	Batch
	// PartitionConcurrent has one message sent to the customer per partition and customer needs to ACK the message
	// to receive the next message for the partition.
	PartitionConcurrent
)

// NoSleep can be set to SubscriberConfig.NackResendSleep and SubscriberConfig.ReconnectRetrySleep.
const NoSleep time.Duration = -1

func (c *SubscriberConfig) setDefaults() {
	if c.OverwriteSaramaConfig == nil {
		c.OverwriteSaramaConfig = DefaultSaramaSubscriberConfig()
	}
	if c.NackResendSleep == 0 {
		c.NackResendSleep = time.Millisecond * 100
	}
	if c.ReconnectRetrySleep == 0 {
		c.ReconnectRetrySleep = time.Second
	}
	switch c.ConsumerModel {
	case Batch:
		if c.BatchConsumerConfig == nil {
			c.BatchConsumerConfig = &BatchConsumerConfig{}
		}
		if c.BatchConsumerConfig.MaxBatchSize == 0 {
			c.BatchConsumerConfig.MaxBatchSize = 100
		}
		if c.BatchConsumerConfig.MaxWaitTime == 0 {
			c.BatchConsumerConfig.MaxWaitTime = time.Millisecond * 100
		}
	}
}

func (c SubscriberConfig) Validate() error {
	if len(c.Brokers) == 0 {
		return errors.New("missing brokers")
	}
	if c.Unmarshaler == nil {
		return errors.New("missing unmarshaler")
	}

	return nil
}

// DefaultSaramaSubscriberConfig creates default Sarama config used by Watermill.
//
// Custom config can be passed to NewSubscriber and NewPublisher.
//
//	saramaConfig := DefaultSaramaSubscriberConfig()
//	saramaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
//
//	subscriberConfig.OverwriteSaramaConfig = saramaConfig
//
//	subscriber, err := NewSubscriber(subscriberConfig, logger)
//	// ...
func DefaultSaramaSubscriberConfig() *sarama.Config {
	config := sarama.NewConfig()
	config.Version = sarama.V1_0_0_0
	config.Consumer.Return.Errors = true
	config.ClientID = "watermill"

	return config
}

// Subscribe subscribers for messages in Kafka.
//
// There are multiple subscribers spawned
func (s *Subscriber) Subscribe(ctx context.Context, topic string) (<-chan *message.Message, error) {
	if s.closed {
		return nil, errors.New("subscriber closed")
	}

	s.subscribersWg.Add(1)

	logFields := watermill.LogFields{
		"provider":            "kafka",
		"topic":               topic,
		"consumer_group":      s.config.ConsumerGroup,
		"kafka_consumer_uuid": watermill.NewShortUUID(),
	}
	s.logger.Info("Subscribing to Kafka topic", logFields)

	// we don't want to have buffered channel to not consume message from Kafka when consumer is not consuming
	output := make(chan *message.Message)

	consumeClosed, err := s.consumeMessages(ctx, topic, output, logFields)
	if err != nil {
		s.subscribersWg.Done()
		return nil, err
	}

	go func() {
		// blocking, until s.closing is closed
		s.handleReconnects(ctx, topic, output, consumeClosed, logFields)
		close(output)
		s.subscribersWg.Done()
	}()

	return output, nil
}

func (s *Subscriber) handleReconnects(
	ctx context.Context,
	topic string,
	output chan *message.Message,
	consumeClosed chan struct{},
	logFields watermill.LogFields,
) {
	for {
		// nil channel will cause deadlock
		if consumeClosed != nil {
			<-consumeClosed
			s.logger.Debug("consumeMessages stopped", logFields)
		} else {
			s.logger.Debug("empty consumeClosed", logFields)
		}

		select {
		// it's important to don't exit before consumeClosed,
		// to not trigger s.subscribersWg.Done() before consumer is closed
		case <-s.closing:
			s.logger.Debug("Closing subscriber, no reconnect needed", logFields)
			return
		case <-ctx.Done():
			s.logger.Debug("Ctx cancelled, no reconnect needed", logFields)
			return
		default:
			s.logger.Debug("Not closing, reconnecting", logFields)
		}

		s.logger.Info("Reconnecting consumer", logFields)

		var err error
		consumeClosed, err = s.consumeMessages(ctx, topic, output, logFields)
		if err != nil {
			s.logger.Error("Cannot reconnect messages consumer", err, logFields)

			if s.config.ReconnectRetrySleep != NoSleep {
				time.Sleep(s.config.ReconnectRetrySleep)
			}
			continue
		}
	}
}

func (s *Subscriber) consumeMessages(
	ctx context.Context,
	topic string,
	output chan *message.Message,
	logFields watermill.LogFields,
) (consumeMessagesClosed chan struct{}, err error) {
	s.logger.Info("Starting consuming", logFields)

	// Start with a client
	client, err := sarama.NewClient(s.config.Brokers, s.config.OverwriteSaramaConfig)
	if err != nil {
		return nil, errors.Wrap(err, "cannot create new Sarama client")
	}

	ctx, cancel := context.WithCancel(ctx)
	go func() {
		select {
		case <-s.closing:
			s.logger.Debug("Closing subscriber, cancelling consumeMessages", logFields)
			cancel()
		case <-ctx.Done():
			// avoid goroutine leak
		}
	}()

	if s.config.ConsumerGroup == "" {
		consumeMessagesClosed, err = s.consumeWithoutConsumerGroups(ctx, client, topic, output, logFields, s.config.Tracer)
	} else {
		consumeMessagesClosed, err = s.consumeGroupMessages(ctx, client, topic, output, logFields, s.config.Tracer)
	}
	if err != nil {
		s.logger.Debug(
			"Starting consume failed, cancelling context",
			logFields.Add(watermill.LogFields{"err": err}),
		)
		cancel()
		return nil, err
	}

	go func() {
		<-consumeMessagesClosed
		if err := client.Close(); err != nil {
			s.logger.Error("Cannot close client", err, logFields)
		} else {
			s.logger.Debug("Client closed", logFields)
		}
	}()

	return consumeMessagesClosed, nil
}

func (s *Subscriber) consumeGroupMessages(
	ctx context.Context,
	client sarama.Client,
	topic string,
	output chan *message.Message,
	logFields watermill.LogFields,
	tracer SaramaTracer,
) (chan struct{}, error) {
	// Start a new consumer group
	group, err := sarama.NewConsumerGroupFromClient(s.config.ConsumerGroup, client)
	if err != nil {
		return nil, errors.Wrap(err, "cannot create consumer group client")
	}

	groupClosed := make(chan struct{})

	handleGroupErrorsCtx, cancelHandleGroupErrors := context.WithCancel(context.Background())
	handleGroupErrorsDone := s.handleGroupErrors(handleGroupErrorsCtx, group, logFields)

	var handler sarama.ConsumerGroupHandler = consumerGroupHandler{
		ctx:              ctx,
		messageHandler:   s.createMessagesHandler(output),
		logger:           s.logger,
		closing:          s.closing,
		messageLogFields: logFields,
	}

	if tracer != nil {
		handler = tracer.WrapConsumerGroupHandler(handler)
	}

	go func() {
		defer func() {
			cancelHandleGroupErrors()
			<-handleGroupErrorsDone

			if err := group.Close(); err != nil {
				s.logger.Info("Group close with error", logFields.Add(watermill.LogFields{"err": err.Error()}))
			}

			s.logger.Info("Consuming done", logFields)
			close(groupClosed)
		}()

	ConsumeLoop:
		for {
			select {
			default:
				s.logger.Debug("Not closing", logFields)
			case <-s.closing:
				s.logger.Debug("Subscriber is closing, stopping group.Consume loop", logFields)
				break ConsumeLoop
			case <-ctx.Done():
				s.logger.Debug("Ctx was cancelled, stopping group.Consume loop", logFields)
				break ConsumeLoop
			}

			if err := group.Consume(ctx, []string{topic}, handler); err != nil {
				if err == sarama.ErrUnknown {
					// this is info, because it is often just noise
					s.logger.Info("Received unknown Sarama error", logFields.Add(watermill.LogFields{"err": err.Error()}))
				} else {
					s.logger.Error("Group consume error", err, logFields)
				}

				break ConsumeLoop
			}

			// this is expected behaviour to run Consume again after it exited
			// see: https://github.com/ThreeDotsLabs/watermill/issues/210
			s.logger.Debug("Consume stopped without any error, running consume again", logFields)
		}
	}()

	return groupClosed, nil
}

func (s *Subscriber) handleGroupErrors(
	ctx context.Context,
	group sarama.ConsumerGroup,
	logFields watermill.LogFields,
) chan struct{} {
	done := make(chan struct{})

	go func() {
		defer close(done)
		errs := group.Errors()

		for {
			select {
			case err := <-errs:
				if err == nil {
					continue
				}

				s.logger.Error("Sarama internal error", err, logFields)
			case <-ctx.Done():
				return
			}
		}
	}()

	return done
}

func (s *Subscriber) consumeWithoutConsumerGroups(
	ctx context.Context,
	client sarama.Client,
	topic string,
	output chan *message.Message,
	logFields watermill.LogFields,
	tracer SaramaTracer,
) (chan struct{}, error) {
	consumer, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		return nil, errors.Wrap(err, "cannot create client")
	}

	if tracer != nil {
		consumer = tracer.WrapConsumer(consumer)
	}

	partitions, err := consumer.Partitions(topic)
	if err != nil {
		return nil, errors.Wrap(err, "cannot get partitions")
	}

	partitionConsumersWg := &sync.WaitGroup{}

	for _, partition := range partitions {
		partitionLogFields := logFields.Add(watermill.LogFields{"kafka_partition": partition})

		partitionConsumer, err := consumer.ConsumePartition(topic, partition, s.config.OverwriteSaramaConfig.Consumer.Offsets.Initial)
		if err != nil {
			if err := client.Close(); err != nil && err != sarama.ErrClosedClient {
				s.logger.Error("Cannot close client", err, partitionLogFields)
			}
			return nil, errors.Wrap(err, "failed to start consumer for partition")
		}

		if tracer != nil {
			partitionConsumer = tracer.WrapPartitionConsumer(partitionConsumer)
		}

		messageHandler := s.createMessagesHandler(output)

		partitionConsumersWg.Add(1)
		go s.consumePartition(ctx, partitionConsumer, messageHandler, partitionConsumersWg, partitionLogFields)
	}

	closed := make(chan struct{})
	go func() {
		partitionConsumersWg.Wait()
		close(closed)
	}()

	return closed, nil
}

func (s *Subscriber) consumePartition(
	ctx context.Context,
	partitionConsumer sarama.PartitionConsumer,
	messageHandler MessageHandler,
	partitionConsumersWg *sync.WaitGroup,
	logFields watermill.LogFields,
) {
	defer func() {
		if err := partitionConsumer.Close(); err != nil {
			s.logger.Error("Cannot close partition consumer", err, logFields)
		}
		partitionConsumersWg.Done()
		s.logger.Debug("consumePartition stopped", logFields)

	}()

	kafkaMessages := partitionConsumer.Messages()

	<-messageHandler.ProcessMessages(ctx, kafkaMessages, nil, logFields)
}

func (s *Subscriber) createMessagesHandler(output chan *message.Message) MessageHandler {
	switch s.config.ConsumerModel {
	case Default:
		return NewMessageHandler(
			output,
			s.config.Unmarshaler,
			s.logger,
			s.closing,
			s.config.NackResendSleep,
		)
	case Batch:
		return NewBatchedMessageHandler(
			output,
			s.config.Unmarshaler,
			s.logger,
			s.closing,
			s.config.BatchConsumerConfig.MaxBatchSize,
			s.config.BatchConsumerConfig.MaxWaitTime,
			s.config.NackResendSleep,
		)
	case PartitionConcurrent:
		return NewConcurrentMessageHandler(output, s.config.Unmarshaler, s.logger, s.closing, s.config.NackResendSleep)
	default:
		panic("Invalid configuration")
	}
}

func (s *Subscriber) Close() error {
	if s.closed {
		return nil
	}

	s.closed = true
	close(s.closing)
	s.subscribersWg.Wait()

	s.logger.Debug("Kafka subscriber closed", nil)

	return nil
}

type consumerGroupHandler struct {
	ctx              context.Context
	messageHandler   MessageHandler
	logger           watermill.LoggerAdapter
	closing          chan struct{}
	messageLogFields watermill.LogFields
}

func (consumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error { return nil }

func (consumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }

func (h consumerGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	logFields := h.messageLogFields.Copy().Add(watermill.LogFields{
		"kafka_partition":      claim.Partition(),
		"kafka_initial_offset": claim.InitialOffset(),
	})

	return <-h.messageHandler.ProcessMessages(h.ctx, claim.Messages(), sess, logFields)
}

func (s *Subscriber) SubscribeInitialize(topic string) (err error) {
	if s.config.InitializeTopicDetails == nil {
		return errors.New("s.config.InitializeTopicDetails is empty, cannot SubscribeInitialize")
	}

	clusterAdmin, err := sarama.NewClusterAdmin(s.config.Brokers, s.config.OverwriteSaramaConfig)
	if err != nil {
		return errors.Wrap(err, "cannot create cluster admin")
	}
	defer func() {
		if closeErr := clusterAdmin.Close(); closeErr != nil {
			err = multierror.Append(err, closeErr)
		}
	}()

	if err := clusterAdmin.CreateTopic(topic, s.config.InitializeTopicDetails, false); err != nil && !strings.Contains(err.Error(), "Topic with this name already exists") {
		return errors.Wrap(err, "cannot create topic")
	}

	s.logger.Info("Created Kafka topic", watermill.LogFields{"topic": topic})

	return nil
}

type PartitionOffset map[int32]int64

func (s *Subscriber) PartitionOffset(topic string) (PartitionOffset, error) {
	client, err := sarama.NewClient(s.config.Brokers, s.config.OverwriteSaramaConfig)
	if err != nil {
		return nil, errors.Wrap(err, "cannot create new Sarama client")
	}

	defer func() {
		if closeErr := client.Close(); closeErr != nil {
			err = multierror.Append(err, closeErr)
		}
	}()

	partitions, err := client.Partitions(topic)
	if err != nil {
		return nil, errors.Wrap(err, "cannot get topic partitions")
	}

	partitionOffset := make(PartitionOffset, len(partitions))
	for _, partition := range partitions {
		offset, err := client.GetOffset(topic, partition, sarama.OffsetNewest)
		if err != nil {
			return nil, err
		}

		partitionOffset[partition] = offset
	}

	return partitionOffset, nil
}
