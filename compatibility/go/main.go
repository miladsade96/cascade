package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	bootstrap := os.Getenv("CASCADE_BOOTSTRAP_SERVERS")
	if bootstrap == "" {
		bootstrap = "127.0.0.1:19092"
	}
	topic := fmt.Sprintf("go-compat-%d", time.Now().UnixMilli())
	if len(os.Args) > 1 {
		topic = os.Args[1]
	}
	expected := make([]string, 25)
	for index := range expected {
		expected[index] = fmt.Sprintf("go-%d", index)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	producer, err := kgo.NewClient(
		kgo.SeedBrokers(bootstrap),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.MaxProduceRequestsInflightPerBroker(1),
	)
	if err != nil {
		return fmt.Errorf("create franz-go producer: %w", err)
	}
	createRequest := kmsg.NewPtrCreateTopicsRequest()
	createRequest.TimeoutMillis = 15_000
	requestedTopic := kmsg.NewCreateTopicsRequestTopic()
	requestedTopic.Topic = topic
	requestedTopic.NumPartitions = 1
	requestedTopic.ReplicationFactor = 1
	createRequest.Topics = append(createRequest.Topics, requestedTopic)
	createResponse, err := createRequest.RequestWith(ctx, producer)
	if err != nil {
		producer.Close()
		return fmt.Errorf("create topic with franz-go: %w", err)
	}
	if len(createResponse.Topics) != 1 || createResponse.Topics[0].ErrorCode != 0 {
		producer.Close()
		return fmt.Errorf("create topic with franz-go: response=%+v", createResponse.Topics)
	}
	records := make([]*kgo.Record, len(expected))
	for index, value := range expected {
		records[index] = &kgo.Record{Topic: topic, Value: []byte(value)}
	}
	results := producer.ProduceSync(ctx, records...)
	producer.Close()
	if err := results.FirstErr(); err != nil {
		return fmt.Errorf("produce with franz-go: %w", err)
	}

	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(bootstrap),
		kgo.ConsumerGroup(topic+"-readers"),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.Balancers(kgo.RoundRobinBalancer()),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		return fmt.Errorf("create franz-go consumer: %w", err)
	}
	defer consumer.Close()

	actual := make([]string, 0, len(expected))
	consumed := make([]*kgo.Record, 0, len(expected))
	for len(actual) < len(expected) {
		fetches := consumer.PollFetches(ctx)
		if err := fetches.Err(); err != nil {
			return fmt.Errorf("consume with franz-go: %w", err)
		}
		fetches.EachRecord(func(record *kgo.Record) {
			if len(actual) < len(expected) {
				actual = append(actual, string(record.Value))
				consumed = append(consumed, record)
			}
		})
	}
	if err := consumer.CommitRecords(ctx, consumed...); err != nil {
		return fmt.Errorf("commit franz-go offsets: %w", err)
	}
	for index := range expected {
		if actual[index] != expected[index] {
			return fmt.Errorf("franz-go mismatch at %d: expected=%q actual=%q", index, expected[index], actual[index])
		}
	}
	fmt.Printf("franz-go verified %d/%d records through %s\n", len(actual), len(expected), bootstrap)
	return nil
}
