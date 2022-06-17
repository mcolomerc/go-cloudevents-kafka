package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"mcolomerc/kafka-client/config"
	event "mcolomerc/kafka-client/events"
	"mcolomerc/kafka-client/kafka_pb"
	"sync/atomic"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/google/uuid"
)

const (
	count = 100
)

func main() {

	config, err := config.LoadConfig("./.env")
	if err != nil {
		log.Fatal("cannot load config:", err)
	}
	log.Println(config)

	// With NewProtocol you can use the same client both to send and receive.
	protocol, err := kafka_pb.NewProtocol(&config)
	if err != nil {
		log.Fatalf("failed to create protocol: %s", err.Error())
	}

	defer protocol.Close(context.Background())

	c, err := cloudevents.NewClient(protocol, cloudevents.WithTimeNow(), cloudevents.WithUUIDs())
	if err != nil {
		log.Fatalf("failed to create client, %v", err)
	}

	// Create a done channel to block until we've received (count) messages
	done := make(chan struct{})

	// Start the consumer
	go func() {
		log.Printf("will listen consuming topic\n")
		var recvCount int32
		err = c.StartReceiver(context.TODO(), func(ctx context.Context, event cloudevents.Event) {
			consume(ctx, event)
			if atomic.AddInt32(&recvCount, 1) == count {
				done <- struct{}{}
			}
		})
		if err != nil {
			log.Fatalf("failed to start receiver: %s", err)
		} else {
			log.Printf("receiver stopped\n")
		}
	}()

	for i := 0; i < count; i++ {
		evtn := event.New()
		jsonEvtn, err := json.Marshal(evtn)
		e := cloudevents.NewEvent()
		e.SetID(uuid.New().String())
		e.SetType("mcolomerc/kafka-producer/event")
		e.SetSource(evtn.Hostname)
		if err != nil {
			log.Fatalf("Error occured during marshaling. Error: %s", err.Error())
		}
		_ = e.SetData(cloudevents.ApplicationJSON, jsonEvtn)

		if result := c.Send(
			// Set the producer message key
			kafka_pb.WithMessageKey(context.Background(), kafka_pb.StringEncoder(e.ID())),
			e,
		); cloudevents.IsUndelivered(result) {
			log.Printf("failed to send: %v", result)
		} else {
			log.Printf("sent: %d, accepted: %t", i, cloudevents.IsACK(result))
		}
	}

}

func consume(ctx context.Context, event cloudevents.Event) {
	fmt.Printf("Received CloudEvent: %s", event)
}
