package kafka_pb

import (
	"context"

	"mcolomerc/kafka-client/config"
	"sync"

	"github.com/cloudevents/sdk-go/v2/binding"
	"github.com/cloudevents/sdk-go/v2/protocol"
)

const (
	defaultGroupId = "cloudevents-sdk-go"
)

type Protocol struct {
	// Sender
	Sender *Sender

	// Sender options
	SenderContextDecorators []func(context.Context) context.Context
	senderTopic             string

	// Consumer
	Consumer        *Consumer
	consumerMux     sync.Mutex
	receiverGroupId string
}

// NewProtocol creates a new kafka transport.
func NewProtocol(kafkaConfig *config.Config, opts ...ProtocolOptionFunc) (*Protocol, error) {

	var err error

	sender, err := NewSender(kafkaConfig)
	if err != nil {
		return nil, err
	}
	receiver, err := NewConsumer(kafkaConfig)
	if err != nil {
		return nil, err
	}

	p := &Protocol{
		Sender:                  sender,
		SenderContextDecorators: make([]func(context.Context) context.Context, 0),
		senderTopic:             kafkaConfig.TOPIC,
		Consumer:                receiver,
	}

	if err = p.applyOptions(opts...); err != nil {
		return nil, err
	}

	return p, nil
}

func (p *Protocol) applyOptions(opts ...ProtocolOptionFunc) error {
	for _, fn := range opts {
		fn(p)
	}
	return nil
}

// OpenInbound implements Opener.OpenInbound
// NOTE: This is a blocking call.
func (p *Protocol) OpenInbound(ctx context.Context) error {
	p.consumerMux.Lock()
	defer p.consumerMux.Unlock()
	return p.Consumer.OpenInbound(ctx)
}

func (p *Protocol) Send(ctx context.Context, in binding.Message, transformers ...binding.Transformer) error {

	for _, f := range p.SenderContextDecorators {
		ctx = f(ctx)
	}
	return p.Sender.Send(ctx, in, transformers...)
}

func (p *Protocol) Receive(ctx context.Context) (binding.Message, error) {

	return p.Consumer.Receive(ctx)
}

func (p *Protocol) Close(ctx context.Context) error {
	return p.Sender.Close(ctx)
}

// Kafka protocol implements Sender, Receiver
var _ protocol.Sender = (*Protocol)(nil)
var _ protocol.Receiver = (*Protocol)(nil)
var _ protocol.Closer = (*Protocol)(nil)
