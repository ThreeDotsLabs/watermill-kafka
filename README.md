# Watermill Kafka Pub/Sub
<img align="right" width="200" src="https://watermill.io/img/gopher.svg">

[![CI Status](https://github.com/ThreeDotsLabs/watermill-kafka/actions/workflows/master.yml/badge.svg)](https://github.com/ThreeDotsLabs/watermill-kafka/actions/workflows/master.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/ThreeDotsLabs/watermill-kafka)](https://goreportcard.com/report/github.com/ThreeDotsLabs/watermill-kafka)

This is Pub/Sub for the [Watermill](https://watermill.io/) project.


See [DEVELOPMENT.md](./DEVELOPMENT.md) for more information about running and testing.

Watermill is a Go library for working efficiently with message streams. It is intended
for building event driven applications, enabling event sourcing, RPC over messages,
sagas and basically whatever else comes to your mind. You can use conventional pub/sub
implementations like Kafka or RabbitMQ, but also HTTP or MySQL binlog if that fits your use case.

All Pub/Sub implementations can be found at [https://watermill.io/pubsubs/](https://watermill.io/pubsubs/).

Documentation: https://watermill.io/

Getting started guide: https://watermill.io/docs/getting-started/

Issues: https://github.com/ThreeDotsLabs/watermill/issues

## Message consumption models

The library, complies with the Subscriber interface:

```go
type Subscriber interface {
	Subscribe(ctx context.Context, topic string) (<-chan *Message, error)
	Close() error
}
```

However, it implements several consumption models:
- one in-flight message (default)
- batch consumption
- partition concurrent

### One in-flight message (default)

This is the **default message consumption model**. In this model, when the subscription channel returns a message, it will not return another one until that message is ACKed or NACKed.

When a message is ACKed, the next one (if any), will be pushed to the channel and the partition offset will be updated if there is a consumer group session.

This mode has the advantage of being simple and easily ensuring ordering.

### Batch consumption

While the default model is simple to understand and safe, sometimes, a greater degree of parallelism is required. For example:
- if you use a partitioner based on the key of the messages, you can expect a partial order, that is, the messages overall are not sorted, but they are sorted within the same partition. In that case, you can potentially process multiple messages and the default can fall short
- for some reason you do not care about the order

In this model, the customer can configure a `maxBatchSize` and `maxWaitTime`. The subscriber will wait until there are `maxBatchSize` messages ready or `maxWaitTime` is ellapsed.

It will, then introduce those messages on the subscription channel. That means that a consumer can now get multiple messages without having to ACK / NACK the previously received ones.

This model deals with ACKs and NACKs properly by resetting the offset of the different (topics, partitions) tuples to the last
message ACKed before a NACK for that (topic, partition) arrived.

Some examples:
- all messages ACKed: offset of the latest message is marked as done
- first message ACKed and second NACKed: offset for the first message is marked as done and second message is resent

To configure it:

```go
    kafka.SubscriberConfig{
      // ... other settings here
      ConsumerModel:       kafka.Default,
      BatchConsumerConfig: &kafka.BatchConsumerConfig{
          MaxBatchSize: 10,
          MaxWaitTime:  100 * time.Millisecond,
      },
}

```

### Partition Concurrent

Partition concurrent works similar to the default consumption model. The main difference is that it allows up to N in-flight models, where N is the number of partitions.
This allows higher concurrency of processing while easily preserving order.

To configure it:

```go
    kafka.SubscriberConfig{
      // ... other settings here
      ConsumerModel: kafka.PartitionConcurrent,
}

```

## Contributing

All contributions are very much welcome. If you'd like to help with Watermill development,
please see [open issues](https://github.com/ThreeDotsLabs/watermill/issues?utf8=%E2%9C%93&q=is%3Aissue+is%3Aopen+)
and submit your pull request via GitHub.

## Support

If you didn't find the answer to your question in [the documentation](https://watermill.io/), feel free to ask us directly!

Please join us on the `#watermill` channel on the [Three Dots Labs Discord](https://discord.gg/QV6VFg4YQE).

## License

[MIT License](./LICENSE)
