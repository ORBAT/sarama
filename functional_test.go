package sarama

import (
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"sync"
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

func TestFuncQueuingWriterParallel(t *testing.T) {
	pc := NewProducerConfig()
	defer LogTo(os.Stderr)()
	client, err := NewClient("functional_test", []string{kafkaAddr}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	pc.FlushFrequency = 100 * time.Millisecond
	producer, err := client.NewQueuingWriter("single_partition", pc)
	if err != nil {
		t.Fatal(err)
	}
	testWriterParallel(client, producer, t)
}

func TestFuncQueuingWriterSync(t *testing.T) {
	pc := NewProducerConfig()
	defer LogTo(os.Stderr)()
	client, err := NewClient("functional_test", []string{kafkaAddr}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	pc.FlushFrequency = 100 * time.Millisecond
	producer, err := client.NewQueuingWriter("single_partition", pc)
	if err != nil {
		t.Fatal(err)
	}
	testWriter(client, producer, t)
}

func TestFuncUnsafeWriterParallel(t *testing.T) {
	pc := NewProducerConfig()
	defer LogTo(os.Stderr)()
	client, err := NewClient("functional_test", []string{kafkaAddr}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	pc.FlushFrequency = 100 * time.Millisecond
	producer, err := client.NewUnsafeWriter("single_partition", pc)
	if err != nil {
		t.Fatal(err)
	}
	testWriterParallel(client, producer, t)
}

func TestFuncUnsafeWriterSync(t *testing.T) {
	pc := NewProducerConfig()
	defer LogTo(os.Stderr)()
	client, err := NewClient("functional_test", []string{kafkaAddr}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	pc.FlushFrequency = 100 * time.Millisecond
	producer, err := client.NewUnsafeWriter("single_partition", pc)
	if err != nil {
		t.Fatal(err)
	}
	testWriter(client, producer, t)
}

func testWriter(client *Client, producer io.WriteCloser, t *testing.T) {

	checkKafkaAvailability(t)

	consumerConfig := NewConsumerConfig()
	consumerConfig.OffsetMethod = OffsetMethodNewest

	consumer, err := NewConsumer(client, "single_partition", 0, "functional_test", consumerConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()

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

func testWriterParallel(client *Client, producer io.WriteCloser, t *testing.T) {
	batchSize := 3
	checkKafkaAvailability(t)

	consumerConfig := NewConsumerConfig()
	consumerConfig.OffsetMethod = OffsetMethodNewest

	consumer, err := NewConsumer(client, "single_partition", 0, "functional_test", consumerConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()
	var wg sync.WaitGroup
	wg.Add(batchSize)
	log.Println("added", batchSize)
	for i := 0; i < batchSize; i++ {
		go func(i int, wg *sync.WaitGroup) {
			log.Println(i, "writing")
			defer func() {
				log.Println(i, "done")
				wg.Done()
			}()

			msg := []byte(fmt.Sprintf("%d", i))
			n, err := producer.Write(msg)

			if err != nil {
				t.Fatal(err)
			}
			if n != len(msg) {
				t.Fatal("Wrote", n, "bytes, expected", len(msg))
			}
		}(i, &wg)
	}
	wg.Wait()
	producer.Close()
	recvd := make([]*ConsumerEvent, batchSize)
	events := consumer.Events()
	for i := 0; i < batchSize; i++ {
		select {
		case <-time.After(10 * time.Second):
			t.Fatal("Not received any more events in the last 10 seconds.")

		case event := <-events:
			idx, err := strconv.Atoi(string(event.Value))
			if err != nil {
				t.Fatalf("Expected a string with a number, got %#v (%s). Error was %s", event.Value, string(event.Value), err.Error())
			}
			recvd[idx] = event
		}

	}

	for idx, val := range recvd {
		t.Logf("recvd[%d] = %#v", idx, recvd[idx])
		if val == nil {
			t.Errorf("Never got a message with the number %d", idx)
		}
	}
}

func TestFuncSyncWriter(t *testing.T) {
	pc := NewProducerConfig()
	defer LogTo(os.Stderr)()
	client, err := NewClient("functional_test", []string{kafkaAddr}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	pc.FlushFrequency = 100 * time.Millisecond
	producer, err := client.NewSyncWriter("single_partition", pc)
	if err != nil {
		t.Fatal(err)
	}
	testWriter(client, producer, t)
}

func TestFuncProducing(t *testing.T) {
	config := NewProducerConfig()
	testProducingMessages(t, config)
}

func TestFuncProducingGzip(t *testing.T) {
	config := NewProducerConfig()
	config.Compression = CompressionGZIP
	testProducingMessages(t, config)
}

func TestFuncProducingSnappy(t *testing.T) {
	config := NewProducerConfig()
	config.Compression = CompressionSnappy
	testProducingMessages(t, config)
}

func TestFuncProducingNoResponse(t *testing.T) {
	config := NewProducerConfig()
	config.RequiredAcks = NoResponse
	testProducingMessages(t, config)
}

func TestFuncProducingFlushing(t *testing.T) {
	config := NewProducerConfig()
	config.FlushMsgCount = TestBatchSize / 8
	config.FlushFrequency = 250 * time.Millisecond
	testProducingMessages(t, config)
}

func testProducingMessages(t *testing.T, config *ProducerConfig) {
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

	config.AckSuccesses = true
	producer, err := NewProducer(client, config)
	if err != nil {
		t.Fatal(err)
	}

	expectedResponses := TestBatchSize
	for i := 1; i <= TestBatchSize; {
		msg := &MessageToSend{Topic: "single_partition", Key: nil, Value: StringEncoder(fmt.Sprintf("testing %d", i))}
		select {
		case producer.Input() <- msg:
			i++
		case ret := <-producer.Errors():
			if ret.Err == nil {
				expectedResponses--
			} else {
				t.Fatal(ret.Err)
			}
		}
	}
	for expectedResponses > 0 {
		ret := <-producer.Errors()
		if ret.Err == nil {
			expectedResponses--
		} else {
			t.Fatal(ret.Err)
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
