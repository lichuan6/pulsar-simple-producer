package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
)

var (
	topic string
	count int
)

func init() {
	const (
		defaultTopic = "default"
		usage        = "the topic of pulsar"
	)
	flag.StringVar(&topic, "topic", defaultTopic, usage)
	flag.IntVar(&count, "count", 10, "how many messages to send")
}

func main() {
	flag.Parse()
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:               "pulsar://localhost:6650",
		OperationTimeout:  30 * time.Second,
		ConnectionTimeout: 30 * time.Second,
	})
	if err != nil {
		log.Fatalf("Could not instantiate Pulsar client: %v", err)
	}
	defer client.Close()

	produceBatchMessagesWithCount(client, topic, count)

	if err != nil {
		fmt.Println("Failed to publish message", err)
	}
	fmt.Println("Published message")
}

func produceBatchMessagesWithCount(client pulsar.Client, topic string, count int) error {
	log.Printf("Topic : %s, Count to send: %d\n", topic, count)
	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic: topic,
	})
	if err != nil {
		return err
	}
	defer producer.Close()

	for i := 0; i < count; i++ {
        m := fmt.Sprintf(`{"@timestamp":"2021-02-22T15:07:03+08:00","data": "%d", "log":"prefix%d"}`, i, i)
		// log.Printf("MSG: %s\n", m)
		producer.SendAsync(context.Background(), &pulsar.ProducerMessage{
			Payload: []byte(m),
		}, func(id pulsar.MessageID, producerMessage *pulsar.ProducerMessage, e error) {
			if e != nil {
				log.Printf("Failed to publish, error %v\n", e)
			} else {
				// log.Printf("Published message %v\n", id)
			}
		})
	}

	log.Printf("Produce %d messages DONE\n", count)

	if err = producer.Flush(); err != nil {
		log.Printf("Failed to Flush, error %v\n", err)
		return err
	}

	return nil
}
