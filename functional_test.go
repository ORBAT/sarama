package sarama

import (
	"fmt"
	"net"
	"os"
	"testing"
	"time"
)

const (
	TestBatchSize = 1000
)

var (
	kafkaIsAvailable, kafkaShouldBeAvailable bool
	kafkaAddr                                string
)

func init() {
	kafkaAddr = os.Getenv("KAFKA_ADDR")
	if kafkaAddr == "" {
		kafkaAddr = "localhost:6667"
	}

	c, err := net.Dial("tcp", kafkaAddr)
	if err == nil {
		kafkaIsAvailable = true
		c.Close()
	}

	kafkaShouldBeAvailable = os.Getenv("CI") != ""
}

func checkKafkaAvailability(t *testing.T) {
	if !kafkaIsAvailable {
		if kafkaShouldBeAvailable {
			t.Fatalf("Kafka broker is not available on %s. Set KAFKA_ADDR to connect to Kafka on a different location.", kafkaAddr)
		} else {
			t.Skipf("Kafka broker is not available on %s. Set KAFKA_ADDR to connect to Kafka on a different location.", kafkaAddr)
		}
	}
}

func TestUnsafeWriterFunctional(t *testing.T) {

	checkKafkaAvailability(t)

	client, err := NewClient("functional_test", []string{kafkaAddr}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	consumerConfig := NewConsumerConfig()
	consumerConfig.OffsetMethod = OffsetMethodNewest

	consumer, err := NewConsumer(client, "single_partition", 0, "functional_test", consumerConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()
	pc := NewProducerConfig()
	pc.MaxBufferTime = 100 * time.Millisecond
	producer, err := client.NewUnsafeWriter("single_partition", nil)
	if err != nil {
		t.Fatal(err)
	}
	var msg []byte
	for i := 1; i <= TestBatchSize; i++ {
		msg = []byte(fmt.Sprintf("testing %d", i))
		n, err := producer.Write(msg)
		if err != nil {
			t.Fatal(err)
		}
		if n != len(msg) {
			t.Fatal("Wrote", n, "bytes, expected", len(msg))
		}
	}
	producer.Close()

	events := consumer.Events()
	for i := 1; i <= TestBatchSize; i++ {
		select {
		case <-time.After(10 * time.Second):
			t.Fatal("Not received any more events in the last 10 seconds.")

		case event := <-events:
			if string(event.Value) != fmt.Sprintf("testing %d", i) {
				t.Fatalf("Unexpected message with index %d: %s", i, event.Value)
			}
		}

	}

}

func TestSyncWriterFunctional(t *testing.T) {

	checkKafkaAvailability(t)

	client, err := NewClient("functional_test", []string{kafkaAddr}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	consumerConfig := NewConsumerConfig()
	consumerConfig.OffsetMethod = OffsetMethodNewest

	consumer, err := NewConsumer(client, "single_partition", 0, "functional_test", consumerConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()

	producer, err := client.NewSyncWriter("single_partition", nil)
	if err != nil {
		t.Fatal(err)
	}
	var msg []byte
	for i := 1; i <= TestBatchSize; i++ {
		msg = []byte(fmt.Sprintf("testing %d", i))
		n, err := producer.Write(msg)
		if err != nil {
			t.Fatal(err)
		}
		if n != len(msg) {
			t.Fatal("Wrote", n, "bytes, expected", len(msg))
		}
	}
	producer.Close()

	events := consumer.Events()
	for i := 1; i <= TestBatchSize; i++ {
		select {
		case <-time.After(10 * time.Second):
			t.Fatal("Not received any more events in the last 10 seconds.")

		case event := <-events:
			if string(event.Value) != fmt.Sprintf("testing %d", i) {
				t.Fatalf("Unexpected message with index %d: %s", i, event.Value)
			}
		}

	}

}

func TestProducingMessages(t *testing.T) {
	checkKafkaAvailability(t)

	client, err := NewClient("functional_test", []string{kafkaAddr}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	consumerConfig := NewConsumerConfig()
	consumerConfig.OffsetMethod = OffsetMethodNewest

	consumer, err := NewConsumer(client, "single_partition", 0, "functional_test", consumerConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()

	producer, err := NewProducer(client, nil)
	if err != nil {
		t.Fatal(err)
	}

	for i := 1; i <= TestBatchSize; i++ {
		err = producer.SendMessage("single_partition", nil, StringEncoder(fmt.Sprintf("testing %d", i)))
		if err != nil {
			t.Fatal(err)
		}
	}
	producer.Close()

	events := consumer.Events()
	for i := 1; i <= TestBatchSize; i++ {
		select {
		case <-time.After(10 * time.Second):
			t.Fatal("Not received any more events in the last 10 seconds.")

		case event := <-events:
			if string(event.Value) != fmt.Sprintf("testing %d", i) {
				t.Fatalf("Unexpected message with index %d: %s", i, event.Value)
			}
		}

	}
}
