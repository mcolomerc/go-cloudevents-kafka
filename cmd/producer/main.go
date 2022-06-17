package main

import (
	"encoding/json"
	"log"
	"mcolomerc/kafka-client/config"
	event "mcolomerc/kafka-client/events"
	"mcolomerc/kafka-client/producer"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {

	config, err := config.LoadConfig("./.env")
	if err != nil {
		log.Fatal("cannot load config:", err)
	}
	log.Println("Config ::", config)

	// Create Producer instance
	producer := producer.New(&config)

	evtn := event.New()
	jsonEvtn, err := json.Marshal(evtn)
	if err != nil {
		log.Fatal("Error ", err)
	}

	message := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &config.TOPIC, Partition: kafka.PartitionAny},
		Value:          jsonEvtn,
		Headers:        []kafka.Header{{Key: "myTestHeader", Value: []byte("header values are binary")}},
	}

	producer.SendMessage(message)
}
