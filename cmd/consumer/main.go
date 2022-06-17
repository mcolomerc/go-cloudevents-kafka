package main

import (
	"fmt"
	"log"
	"mcolomerc/kafka-client/config"
	"mcolomerc/kafka-client/consumer"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	config, err := config.LoadConfig("./.env")
	if err != nil {
		log.Fatal("cannot load config:", err)
	}
	log.Println("Config ::", config)

	msgCh := make(chan kafka.Message)

	consumer := consumer.New(&config)
	go consumer.Consume(msgCh)

	for {
		message := <-msgCh
		fmt.Println("message: ", string(message.Value))
		fmt.Println("partition: ", string(message.TopicPartition.Partition))
	}

}
