package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
)

var topic string

func init() {
	const (
		defaultTopic = "default"
		usage        = "the topic of pulsar"
	)
	flag.StringVar(&topic, "topic", defaultTopic, usage)
}


func consumerAllHandleInterrupt(topic string) {
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:               "pulsar://localhost:6650",
		OperationTimeout:  30 * time.Second,
		ConnectionTimeout: 30 * time.Second,
	})
	if err != nil {
		log.Fatalf("Could not instantiate Pulsar client: %v", err)
	}

	defer client.Close()

	msgChannel := make(chan pulsar.ConsumerMessage)
	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:                       topic,
		SubscriptionName:            fmt.Sprintf("my-sub-%s", topic),
		Type:                        pulsar.Shared,
		MessageChannel:              msgChannel,
		SubscriptionInitialPosition: pulsar.SubscriptionPositionLatest,
	})

	if err != nil {
		log.Fatalf("client.Subscribe error : %v", err)
	}
	defer consumer.Close()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)

	var count uint32
	for {
		select {
		case s := <-c:
			fmt.Println("Got signal: ", s)
			fmt.Printf("Count : %d", count)
			// unsubscribe
			if err := consumer.Unsubscribe(); err != nil {
				log.Fatal(err)
			}
			return
		case cm := <-msgChannel:
			count++
			msg := cm.Message

			// deserialized to struct
			type T struct {
				Log string `json:"log"`
			}

			t := T{}
			err := json.Unmarshal(msg.Payload(), &t)
			if err != nil {
				log.Fatalf("json.unmarshal error %v\n", err)
			}

			consumer.Ack(msg)

			if count%100000 == 0 {
				fmt.Printf("%d Consumed")
			}
			if count == 500000 {
				fmt.Println("500000 Consumed")
			}
		}
	}
}

func main() {
	flag.Parse()
	log.Printf("Topic is : %s\n", topic)

	consumerAllHandleInterrupt("public/default/test")
}
