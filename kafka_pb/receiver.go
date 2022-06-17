package kafka_pb

import (
	"context"

	"io"
	"mcolomerc/kafka-client/config"
	"mcolomerc/kafka-client/consumer"
	"sync"

	"github.com/cloudevents/sdk-go/v2/binding"
	"github.com/cloudevents/sdk-go/v2/protocol"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type msgErr struct {
	msg binding.Message
	err error
}

type Receiver struct {
	once     sync.Once
	incoming chan msgErr
	msgCh    chan *kafka.Message
}

func NewReceiver() *Receiver {
	return &Receiver{
		incoming: make(chan msgErr),
		msgCh:    make(chan *kafka.Message),
	}
}

func (r *Receiver) Close(context.Context) error {
	r.once.Do(func() {
		close(r.incoming)
	})
	return nil
}

func (r *Receiver) MsgHandler(msg *kafka.Message) {
	r.incoming <- msgErr{msg: NewMessageFromConsumerMessage(msg)}
}

func (r *Receiver) Receive(ctx context.Context) (binding.Message, error) {

	select {
	case <-ctx.Done():
		return nil, io.EOF
	case msgErr, ok := <-r.incoming:
		if !ok {
			return nil, io.EOF
		}
		return msgErr.msg, msgErr.err
	}
}

func (r *Receiver) Setup() error {
	return nil
}

func (r *Receiver) Cleanup() error {
	return nil
}

var _ protocol.Receiver = (*Receiver)(nil)
var _ protocol.Closer = (*Receiver)(nil)

type Consumer struct {
	Receiver
	kafkaConsumer *consumer.Consumer
	topic         string
	groupId       string
}

func NewConsumer(config *config.Config) (*Consumer, error) {
	kafkaconsumer := consumer.New(config)
	consumer := &Consumer{
		Receiver:      *NewReceiver(),
		kafkaConsumer: kafkaconsumer,
		topic:         config.TOPIC,
		groupId:       config.GROUP_ID,
	}
	return consumer, nil
}

func (c *Consumer) OpenInbound(ctx context.Context) error {
	go c.kafkaConsumer.Consume(c.Receiver.msgCh)
	for {
		msg := <-c.Receiver.msgCh
		c.Receiver.MsgHandler(msg)
	}
}

func (c *Consumer) Close(ctx context.Context) error {
	return c.kafkaConsumer.Close()
}

var _ protocol.Opener = (*Consumer)(nil)
var _ protocol.Closer = (*Consumer)(nil)
