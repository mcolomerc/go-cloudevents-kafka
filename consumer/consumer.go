package consumer

import (
	"fmt"
	"mcolomerc/kafka-client/config"
	"os"
	"os/signal"
	"syscall"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type Consumer struct { // struct for producer
	topic    string
	consumer *kafka.Consumer
}

func New(config *config.Config) *Consumer {
	fmt.Println("Creating consumer:: Topic:: " + config.TOPIC)
	// create a new kafka consumer
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":               config.BOOTSTRAP_SERVERS,
		"sasl.mechanisms":                 config.SASL_MECHANISM,
		"security.protocol":               config.SECURITY_PROTOCOL,
		"sasl.username":                   config.SASL_USERNAME,
		"sasl.password":                   config.SASL_PASSWORD,
		"group.id":                        config.GROUP_ID,
		"auto.offset.reset":               config.AUTO_OFFSET_RESET,
		"go.events.channel.enable":        true,
		"go.application.rebalance.enable": true,
		"enable.partition.eof":            true,
	})

	if err != nil {
		fmt.Printf("Failed to create consumer: %s", err)
		os.Exit(1)
	}

	topic := config.TOPIC

	return &Consumer{ // create new producer
		topic:    topic,
		consumer: c,
	}
}

func (c *Consumer) Consume(ch chan (*kafka.Message)) {
	fmt.Printf("consumer.Consume %s", c.topic)
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	c.consumer.SubscribeTopics([]string{c.topic}, nil)
	run := true
	fmt.Println("consumer.Polling...")
	for run == true {
		select {
		case sig := <-sigchan:
			fmt.Printf("Caught signal %v: terminating\n", sig)
			run = false

		case ev := <-c.consumer.Events():
			switch e := ev.(type) {
			case kafka.AssignedPartitions:
				fmt.Fprintf(os.Stderr, "%% %v\n", e)
				c.consumer.Assign(e.Partitions)
			case kafka.RevokedPartitions:
				fmt.Fprintf(os.Stderr, "%% %v\n", e)
				c.consumer.Unassign()
			case *kafka.Message:
				fmt.Printf("%% Message on %s:\n%s\n", e.TopicPartition, string(e.Value))
				ch <- e
			case kafka.PartitionEOF:
				fmt.Printf("%% Reached %v\n", e)
			case kafka.Error:
				// Errors should generally be considered as informational, the client will try to automatically recover
				fmt.Fprintf(os.Stderr, "%% Error: %v\n", e)
			}
		}
	}
	fmt.Printf("Closing consumer\n")
	c.consumer.Close()
}

func (c *Consumer) Close() error {
	c.consumer.Close()
	return nil
}
