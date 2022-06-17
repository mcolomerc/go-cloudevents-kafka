package producer

import (
	"fmt"
	"log"
	"mcolomerc/kafka-client/config"
	"os"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type Producer struct { // struct for producer
	topic    string
	producer *kafka.Producer
}

func New(config *config.Config) *Producer {
	// Create Producer instance
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": config.BOOTSTRAP_SERVERS,
		"sasl.mechanisms":   config.SASL_MECHANISM,
		"security.protocol": config.SECURITY_PROTOCOL,
		"sasl.username":     config.SASL_USERNAME,
		"sasl.password":     config.SASL_PASSWORD})
	if err != nil {
		fmt.Printf("Failed to create producer: %s", err)
		os.Exit(1)
	}
	if err != nil {
		log.Fatal(err)
	}
	topic := config.TOPIC

	return &Producer{ // create new producer
		topic:    topic,
		producer: producer,
	}
}

func (p *Producer) Send(key string, message string, headers map[string]string) error {
	deliveryChan := make(chan (kafka.Event))
	defer close(deliveryChan)

	var kafkaHeaders []kafka.Header // create slice of headers

	for key, value := range headers {
		kafkaHeaders = append(kafkaHeaders, kafka.Header{
			Key:   key,
			Value: []byte(value),
		})
	}

	p.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &p.topic, Partition: kafka.PartitionAny},
		Key:            []byte(key),
		Value:          []byte(message),
		Headers:        kafkaHeaders,
	}, deliveryChan)

	e := <-deliveryChan
	m := e.(*kafka.Message)

	if m.TopicPartition.Error != nil {
		fmt.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
		return m.TopicPartition.Error
	} else {
		fmt.Printf("Delivered message to topic %s [%d] at offset %v\n",
			*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)

	}

	return nil
}

func (p *Producer) SendMessage(message *kafka.Message) error {
	deliveryChan := make(chan (kafka.Event))
	defer close(deliveryChan)

	p.producer.Produce(message, deliveryChan)

	e := <-deliveryChan
	m := e.(*kafka.Message)

	if m.TopicPartition.Error != nil {
		fmt.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
		return m.TopicPartition.Error
	} else {
		fmt.Printf("Delivered message to topic %s [%d] at offset %v\n",
			*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
	}

	return nil
}

func (p *Producer) SendBinary(key []byte, message []byte, headers map[string]string) error {
	deliveryChan := make(chan (kafka.Event))
	defer close(deliveryChan)

	var kafkaHeaders []kafka.Header // create slice of headers

	for key, value := range headers {
		kafkaHeaders = append(kafkaHeaders, kafka.Header{
			Key:   key,
			Value: []byte(value),
		})
	}

	p.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &p.topic, Partition: kafka.PartitionAny},
		Key:            key,
		Value:          message,
		Headers:        kafkaHeaders,
	}, deliveryChan)

	e := <-deliveryChan
	m := e.(*kafka.Message)

	if m.TopicPartition.Error != nil {
		fmt.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
		return m.TopicPartition.Error
	} else {
		fmt.Printf("Delivered message to topic %s [%d] at offset %v\n",
			*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)

	}

	return nil
}

func (p *Producer) Close() error {
	p.producer.Close()
	return nil
}
