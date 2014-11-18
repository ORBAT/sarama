package sarama

import (
	"fmt"
	"io"
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

func TestFuncMultiPartitionProduce(t *testing.T) {
	checkKafkaAvailability(t)
	client, err := NewClient("functional_test", []string{kafkaAddr}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer safeClose(t, client)

	config := NewProducerConfig()
	config.FlushFrequency = 50 * time.Millisecond
	config.FlushMsgCount = 200
	config.ChannelBufferSize = 20
	config.AckSuccesses = true
	producer, err := NewProducer(client, config)
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	wg.Add(TestBatchSize)

	for i := 1; i <= TestBatchSize; i++ {

		go func(i int, w *sync.WaitGroup) {
			defer w.Done()
			msg := &MessageToSend{Topic: "multi_partition", Key: nil, Value: StringEncoder(fmt.Sprintf("hur %d", i))}
			producer.Input() <- msg
			select {
			case ret := <-producer.Errors():
				t.Fatal(ret.Err)
			case <-producer.Successes():
			}
		}(i, &wg)
	}

	wg.Wait()
	if err := producer.Close(); err != nil {
		t.Error(err)
	}
}

func testProducingMessages(t *testing.T, config *ProducerConfig) {
	checkKafkaAvailability(t)

	client, err := NewClient("functional_test", []string{kafkaAddr}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer safeClose(t, client)

	consumerConfig := NewConsumerConfig()
	consumerConfig.OffsetMethod = OffsetMethodNewest

	consumer, err := NewConsumer(client, "single_partition", 0, "functional_test", consumerConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer safeClose(t, consumer)

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
			t.Fatal(ret.Err)
		case <-producer.Successes():
			expectedResponses--
		}
	}
	for expectedResponses > 0 {
		select {
		case ret := <-producer.Errors():
			t.Fatal(ret.Err)
		case <-producer.Successes():
			expectedResponses--
		}
	}
	err = producer.Close()
	if err != nil {
		t.Error(err)
	}

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

func newProdConf() *ProducerConfig {
	pc := NewProducerConfig()
	pc.FlushFrequency = 5 * time.Millisecond
	pc.FlushMsgCount = 200
	pc.ChannelBufferSize = 20
	return pc
}

func TestFuncQueuingWriterParallel(t *testing.T) {
	pc := newProdConf()
	// defer LogTo(os.Stderr)()
	client, err := NewClient("functional_test", []string{kafkaAddr}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	producer, err := client.NewQueuingWriter("single_partition", pc)
	if err != nil {
		t.Fatal(err)
	}
	funcTestWrParallel(client, producer, pc, 10, t)
}

func TestFuncQueuingWriterSingle(t *testing.T) {
	pc := newProdConf()

	// defer LogTo(os.Stderr)()
	client, err := NewClient("functional_test", []string{kafkaAddr}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	producer, err := client.NewQueuingWriter("single_partition", pc)
	if err != nil {
		t.Fatal(err)
	}
	testWriterFunc(client, producer, t)
}

func TestFuncUnsafeWriterParallel(t *testing.T) {
	pc := newProdConf()
	// defer LogTo(os.Stderr)()
	client, err := NewClient("functional_test", []string{kafkaAddr}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	producer, err := client.NewUnsafeWriter("single_partition", pc)
	if err != nil {
		t.Fatal(err)
	}
	funcTestWrParallel(client, producer, pc, 10, t)
}

func TestFuncUnsafeWriterSingle(t *testing.T) {
	pc := newProdConf()
	// defer LogTo(os.Stderr)()
	client, err := NewClient("functional_test", []string{kafkaAddr}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	producer, err := client.NewUnsafeWriter("single_partition", pc)
	if err != nil {
		t.Fatal(err)
	}
	testWriterFunc(client, producer, t)
}

func testWriterFunc(client *Client, producer io.WriteCloser, t *testing.T) {

	checkKafkaAvailability(t)

	consumerConfig := NewConsumerConfig()
	consumerConfig.OffsetMethod = OffsetMethodNewest

	consumer, err := NewConsumer(client, "single_partition", 0, "functional_test", consumerConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()

	var msg []byte
	for i := 0; i < TestBatchSize; i++ {
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
	for i := 0; i < TestBatchSize; i++ {
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

func writeInParallel(w io.Writer, nWorkers, batchSize int, t *testing.T) *sync.WaitGroup {
	var wg sync.WaitGroup
	wg.Add(nWorkers)
	for i := 0; i < nWorkers; i++ {
		go func(i int, workerWg *sync.WaitGroup) {
			var msgWg sync.WaitGroup
			msgWg.Add(batchSize)
			defer workerWg.Done()
			for msgn := 0; msgn < batchSize; msgn++ {
				go func(msgN int, mwg *sync.WaitGroup) {
					defer mwg.Done()
					msg := []byte(fmt.Sprintf("%d", (i*batchSize)+msgN))
					n, err := w.Write(msg)
					if err != nil {
						t.Error("Write error", err)
					}
					if n != len(msg) {
						t.Error("Wrote", n, "bytes, expected", len(msg))
					}
				}(msgn, &msgWg)
			}
			msgWg.Wait()
		}(i, &wg)
	}
	return &wg
}

func funcTestWrParallel(client *Client, w io.WriteCloser, conf *ProducerConfig, nWorkers int, t *testing.T) {
	checkKafkaAvailability(t)

	batchSize := TestBatchSize

	consumerConfig := NewConsumerConfig()
	consumerConfig.OffsetMethod = OffsetMethodNewest

	consumer, err := NewConsumer(client, "single_partition", 0, "functional_test", consumerConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()

	writeInParallel(w, nWorkers, batchSize, t).Wait()

	defer w.Close()
	recvd := make([]*ConsumerEvent, nWorkers*batchSize)
	events := consumer.Events()
	for i := 0; i < nWorkers*batchSize; i++ {
		select {
		case <-time.After(10 * time.Second):
			t.Fatal("Not received any more events in the last 10 seconds.")

		case event := <-events:
			idx, err := strconv.Atoi(string(event.Value))
			if err != nil {
				t.Fatalf("Expected a string with a number, got %#v (%s). Error was %s", event.Value, string(event.Value), err.Error())
			}
			if recvd[idx] != nil {
				t.Errorf("A message with index %d has already been received?", idx)
			}
			recvd[idx] = event
		}

	}

	for idx, val := range recvd {
		// t.Logf("recvd[%d] = %s", idx, string(recvd[idx].Value))
		if val == nil {
			t.Errorf("Never got a message with the number %d", idx)
		}
	}
}
