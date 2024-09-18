package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {

	// pre-requisuties

	const username = "<YOUR USERNAME>"
	const password = "<YOUR PASSWORD>"
	const topic = "<TOPIC>" // e.g. "tron.broadcasted.transactions"

	// end of pre-requisites

	// Kafka consumer configuration
	config := &kafka.ConfigMap{
		"bootstrap.servers":                     "rpk0.bitquery.io:9093,rpk1.bitquery.io:9093,rpk2.bitquery.io:9093",
		"group.id":                              username + "-mygroup",
		"session.timeout.ms":                    30000,
		"security.protocol":                     "SASL_SSL",
		"ssl.ca.location":                       "server.cer.pem",
		"ssl.key.location":                      "client.key.pem",
		"ssl.certificate.location":              "client.cer.pem",
		"ssl.endpoint.identification.algorithm": "none",
		"sasl.mechanisms":                       "SCRAM-SHA-512",
		"sasl.username":                         username,
		"sasl.password":                         password,
		"auto.offset.reset":                     "latest",
		"enable.auto.commit":                    false,
	}

	// Initialize Kafka consumer
	consumer, err := kafka.NewConsumer(config)
	if err != nil {
		fmt.Printf("Failed to create consumer: %s\n", err)
		os.Exit(1)
	}

	// Subscribe to the topic
	err = consumer.SubscribeTopics([]string{topic}, nil)
	if err != nil {
		fmt.Printf("Failed to subscribe to topic: %s\n", err)
		os.Exit(1)
	}

	// Set up a channel to handle shutdown
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	// Poll messages and process them
	run := true
	for run {
		select {
		case sig := <-sigchan:
			fmt.Printf("Caught signal %v: terminating\n", sig)
			run = false
		default:
			ev := consumer.Poll(100)
			if ev == nil {
				continue
			}

			switch e := ev.(type) {
			case *kafka.Message:
				fmt.Print(e)
				processMessage(e)

			case kafka.Error:
				fmt.Printf("Error: %v\n", e)
				if e.Code() == kafka.ErrAllBrokersDown {
					run = false
				}
			default:
				fmt.Printf("Ignored %v\n", e)
			}
		}
	}

	// Close down consumer
	consumer.Close()
}

func processMessage(msg *kafka.Message) {
	fmt.Printf("Received message on topic %s [%d] at offset %v:\n",
		*msg.TopicPartition.Topic, msg.TopicPartition.Partition, msg.TopicPartition.Offset)

	// Try to parse the message value as JSON
	var jsonData interface{}
	err := json.Unmarshal(msg.Value, &jsonData)
	if err != nil {
		fmt.Printf("Error parsing JSON: %v\n", err)
		fmt.Printf("Raw message: %s\n", string(msg.Value))
		return
	}

	// Pretty print the JSON
	prettyJSON, err := json.MarshalIndent(jsonData, "", "  ")
	if err != nil {
		fmt.Printf("Error prettifying JSON: %v\n", err)
		return
	}

	fmt.Printf("Parsed JSON:\n%s\n", string(prettyJSON))

	// Log message data
	logEntry := map[string]interface{}{
		"topic":     *msg.TopicPartition.Topic,
		"partition": msg.TopicPartition.Partition,
		"offset":    msg.TopicPartition.Offset,
		"key":       string(msg.Key),
		"value":     string(prettyJSON),
	}
	fmt.Printf("Log entry: %+v\n", logEntry)
	fmt.Println("----------------------------------------")
}
