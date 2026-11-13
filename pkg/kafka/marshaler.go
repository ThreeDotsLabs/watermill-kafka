package kafka

import (
	"context"

	"github.com/IBM/sarama"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/pkg/errors"
)

const UUIDHeaderKey = "_watermill_message_uuid"

// Marshaler marshals Watermill's message to Kafka message.
type Marshaler interface {
	Marshal(topic string, msg *message.Message) (*sarama.ProducerMessage, error)
}

// Unmarshaler unmarshals Kafka's message to Watermill's message.
// NOTE: Context set here using the Unmarshal function will not be retained;
// use ContextUnmarshaler's UnmarshalWithContext function to set context on
// unmarshal.
type Unmarshaler interface {
	Unmarshal(*sarama.ConsumerMessage) (*message.Message, error)
}

type MarshalerUnmarshaler interface {
	Marshaler
	Unmarshaler
}

// ContextUnmarshaler unmarshals Kafka's message to Watermill's message
// passing the base context into the UnmarshalWithContext function.
// NOTE: If you are implementing this you MUST set context in this function
// or it will be lost.
type ContextUnmarshaler interface {
	UnmarshalWithContext(context.Context, *sarama.ConsumerMessage) (*message.Message, error)
}

type DefaultMarshaler struct{}

func (DefaultMarshaler) Marshal(topic string, msg *message.Message) (*sarama.ProducerMessage, error) {
	if value := msg.Metadata.Get(UUIDHeaderKey); value != "" {
		return nil, errors.Errorf("metadata %s is reserved by watermill for message UUID", UUIDHeaderKey)
	}

	headers := []sarama.RecordHeader{{
		Key:   []byte(UUIDHeaderKey),
		Value: []byte(msg.UUID),
	}}
	for key, value := range msg.Metadata {
		headers = append(headers, sarama.RecordHeader{
			Key:   []byte(key),
			Value: []byte(value),
		})
	}

	return &sarama.ProducerMessage{
		Topic:   topic,
		Value:   sarama.ByteEncoder(msg.Payload),
		Headers: headers,
	}, nil
}

func (DefaultMarshaler) Unmarshal(kafkaMsg *sarama.ConsumerMessage) (*message.Message, error) {
	var messageID string
	metadata := make(message.Metadata, len(kafkaMsg.Headers))

	for _, header := range kafkaMsg.Headers {
		if string(header.Key) == UUIDHeaderKey {
			messageID = string(header.Value)
		} else {
			metadata.Set(string(header.Key), string(header.Value))
		}
	}

	msg := message.NewMessage(messageID, kafkaMsg.Value)
	msg.Metadata = metadata

	return msg, nil
}

func (m DefaultMarshaler) UnmarshalWithContext(ctx context.Context, kafkaMsg *sarama.ConsumerMessage) (*message.Message, error) {
	msg, err := m.Unmarshal(kafkaMsg)
	if err != nil {
		return nil, err
	}
	msg.SetContext(ctx)
	return msg, nil
}

type GeneratePartitionKey func(topic string, msg *message.Message) (string, error)

type kafkaJsonWithPartitioning struct {
	DefaultMarshaler

	generatePartitionKey GeneratePartitionKey
}

func NewWithPartitioningMarshaler(generatePartitionKey GeneratePartitionKey) MarshalerUnmarshaler {
	return kafkaJsonWithPartitioning{generatePartitionKey: generatePartitionKey}
}

func (j kafkaJsonWithPartitioning) Marshal(topic string, msg *message.Message) (*sarama.ProducerMessage, error) {
	kafkaMsg, err := j.DefaultMarshaler.Marshal(topic, msg)
	if err != nil {
		return nil, err
	}

	key, err := j.generatePartitionKey(topic, msg)
	if err != nil {
		return nil, errors.Wrap(err, "cannot generate partition key")
	}
	kafkaMsg.Key = sarama.ByteEncoder(key)

	return kafkaMsg, nil
}
