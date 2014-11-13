package sarama

import (
	"fmt"
	"io"
	"os"

	"sync"
	"testing"
	"time"
)

var clusterBroker = []string{"127.0.0.1:10092", "127.0.0.1:10093", "127.0.0.1:10094"}
var clusterZk = []string{"localhost:22181", "localhost:22182"}
var singleZk = []string{"localhost:2181"}
var singleBroker = []string{"localhost:9092"}

type AllCloser interface {
	CloseAll() error
}

func closeAllOrPanic(bc AllCloser) {
	if err := bc.CloseAll(); err != nil {
		panic(err)
	}
}

func closeOrPanic(p io.Closer) {
	if err := p.Close(); err != nil {
		panic(err)
	}
}

func fastFlushConfig() *ProducerConfig {
	config := NewProducerConfig()
	config.FlushFrequency = 50 * time.Millisecond
	config.FlushMsgCount = 100
	return config
}

func ExampleClient_UnsafeWriter() {
	kc, _ := NewClient("clientId", []string{"localhost:9092"}, nil)

	kp, _ := kc.NewUnsafeWriter("some-topic", nil)
	n, err := kp.Write([]byte("data"))
	if err != nil {
		panic(err)
	}
	fmt.Println("Wrote", n, "bytes to some-topic")

	kp2, _ := kc.NewUnsafeWriter("another-topic", nil)
	n, err = kp2.Write([]byte{1, 2, 3, 4})
	if err != nil {
		panic(err)
	}
	fmt.Println("Wrote", n, "bytes to another-topic")
}

func testUnsafeProd(kp *UnsafeWriter, wg *sync.WaitGroup, t *testing.T) {
	defer wg.Done()
	defer closeOrPanic(kp)
	for i := 0; i < 10; i++ {
		n, err := kp.Write([]byte(TestMessage))
		if n != len(TestMessage) {
			t.Fatalf("wrote %d bytes in write #%d?", n, i)
		}

		if err != nil {
			t.Fatal(err)
		}
	}

	t.Logf("test for producer %s done", kp.id)
}

func TestUnsafeWriterTwoInstances(t *testing.T) {
	t.Skip("Doesn't work")
	defer LogTo(os.Stderr)()
	mb1 := NewMockBroker(t, 1)
	mb2 := NewMockBroker(t, 2)
	defer mb1.Close()
	defer mb2.Close()
	mdr := new(MetadataResponse)
	mdr.AddBroker(mb2.Addr(), mb2.BrokerID())
	mdr.AddTopicPartition("test-topic", 0, 2)
	mb1.Returns(mdr)

	pr := new(ProduceResponse)
	pr.AddTopicPartition("test-topic", 0, NoError)

	for i := 0; i < 20; i++ {
		mb2.Returns(pr)
	}

	kc, err := NewClient("testid", []string{mb1.Addr()}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func(kc *Client) {
		if !kc.Closed() {
			t.Error("Client wasn't closed?")
		}
	}(kc)
	defer closeOrPanic(kc)
	pc := fastFlushConfig()

	kp, err := kc.NewUnsafeWriter("test-topic", pc)
	if err != nil {
		t.Fatal(err)
	}

	kp2, err := kc.NewUnsafeWriter("test-topic", pc)
	if err != nil {
		t.Fatal(err)
	}

	wg := new(sync.WaitGroup)
	wg.Add(2)
	go testUnsafeProd(kp, wg, t)
	go testUnsafeProd(kp2, wg, t)
	wg.Wait()

	<-time.After(5 * time.Millisecond)

	if !kp.Closed() || !kp2.Closed() {
		t.Errorf("Either kp (%t) or kp2 (%t) wasn't closed", kp.Closed(), kp2.Closed())
	}
}

func TestUnsafeWriterOneInstance(t *testing.T) {
	t.Skip("Doesn't work")
	defer LogTo(os.Stderr)()
	mb1 := NewMockBroker(t, 1)
	mb2 := NewMockBroker(t, 2)
	defer mb1.Close()
	defer mb2.Close()

	mdr := new(MetadataResponse)
	mdr.AddBroker(mb2.Addr(), mb2.BrokerID())
	mdr.AddTopicPartition("test-topic", 0, 2)
	mb1.Returns(mdr)

	pr := new(ProduceResponse)
	pr.AddTopicPartition("test-topic", 0, NoError)

	for i := 0; i < 10; i++ {
		mb2.Returns(pr)
	}
	pc := fastFlushConfig()
	kp, err := NewUnsafeWriter("testid2", "test-topic", []string{mb1.Addr()}, pc, nil)
	if err != nil {
		panic(err)
	}
	defer closeAllOrPanic(kp)

	for i := 0; i < 10; i++ {
		n, err := kp.Write([]byte(TestMessage))
		if n != len(TestMessage) {
			t.Errorf("wrote %d bytes in write #%d?", n, i)
		}

		if err != nil {
			panic(err)
		}
	}

	<-time.After(5 * time.Millisecond)
}

func BenchmarkUnsafeWriterNoCompressionCluster(b *testing.B) {
	// defer LogTo(os.Stderr)()
	kc, err := NewClient("no-compr-benchmark-c", clusterBroker, nil)
	if err != nil {
		panic(err)
	}
	conf := fastFlushConfig()
	kp, err := kc.NewUnsafeWriter("no-compr-prod-bench-c", conf)
	testMsg := make([]byte, 255)
	for i := range testMsg {
		testMsg[i] = byte(i)
	}
	defer closeAllOrPanic(kp)
	b.Log("Client created")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		kp.Write(testMsg)
	}
}

func BenchmarkUnsafeWriterNoCompressionSingle(b *testing.B) {
	// defer LogTo(os.Stderr)()
	kc, err := NewClient("no-compr-benchmark", []string{"127.0.0.1:9092"}, nil)
	if err != nil {
		panic(err)
	}
	conf := fastFlushConfig()
	kp, err := kc.NewUnsafeWriter("no-compr-prod-bench", conf)
	testMsg := make([]byte, 255)
	for i := range testMsg {
		testMsg[i] = byte(i)
	}
	defer closeAllOrPanic(kp)
	b.Log("Client created")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		kp.Write(testMsg)
	}
}
