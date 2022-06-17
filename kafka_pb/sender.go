package kafka_pb

import (
	"context"
	"mcolomerc/kafka-client/config"
	"mcolomerc/kafka-client/producer"

	"github.com/cloudevents/sdk-go/v2/binding"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// Sender implements binding.Sender
type Sender struct {
	topic    string
	producer *producer.Producer
}

// NewSender returns a binding.Sender
func NewSender(config *config.Config, options ...SenderOptionFunc) (*Sender, error) {

	// Create Producer instance
	producer := producer.New(config)

	s := &Sender{
		topic:    config.TOPIC,
		producer: producer,
	}
	for _, o := range options {
		o(s)
	}
	return s, nil
}

func (s *Sender) Send(ctx context.Context, m binding.Message, transformers ...binding.Transformer) error {
	var err error
	defer m.Finish(err)

	kafkaMessage := &ProducerMessage{Topic: s.topic}

	if err = WriteProducerMessage(ctx, m, kafkaMessage, transformers...); err != nil {
		return err
	}

	if k := ctx.Value(withMessageKey{}); k != nil {
		encK, _ := k.(Encoder).Encode()
		kafkaMessage.Key = encK
	}

	kMessage := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &kafkaMessage.Topic, Partition: kafkaMessage.Partition},
		Value:          kafkaMessage.Value,
		Key:            kafkaMessage.Key,
		Timestamp:      kafkaMessage.Timestamp,
		TimestampType:  0,
		Opaque:         nil,
		Headers:        kafkaMessage.Headers,
	}

	err = s.producer.SendMessage(kMessage)
	if err != nil {
		return err
	}

	return nil
}

func (s *Sender) Close(ctx context.Context) error {
	// If the Sender was built with NewSenderFromClient, this Close will close only the producer,
	// otherwise it will close the whole client
	return s.producer.Close()
}

type withMessageKey struct{}

// WithMessageKey allows to set the key used when sending the producer message
func WithMessageKey(ctx context.Context, key Encoder) context.Context {
	return context.WithValue(ctx, withMessageKey{}, key)
}
