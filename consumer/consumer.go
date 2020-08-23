package consumer

import (
	"fmt"
	"os"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

var consumer *kafka.Consumer

func InitKafka() error {
	var err error
	consumer, err = kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": os.Getenv("BOOTSTRAP_SERVERS"),
		"group.id":          os.Getenv("GROUP_ID_CONFIG"),
		"auto.offset.reset": "earliest",
	})
	return err
}

func Consume(topics string, msgChan *chan string) error {
	if err := consumer.Subscribe(topics, nil); err != nil {
		return fmt.Errorf("unable to subscribe to %s, error: %w", topics, err)
	}

	for {
		msg, err := consumer.ReadMessage(-1)
		if err == nil {
			fmt.Printf("Message on %s: %s\n", msg.TopicPartition, string(msg.Value))

			// TODO do something later

			*msgChan <- string(msg.Value)
		} else {
			// The client will automatically try to recover from all errors.
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
		}
	}

	consumer.Close()

	return nil
}
