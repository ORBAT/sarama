package sarama

import (
	"fmt"
	"io"
	"sync"
	"testing"
)

var clusterBroker = []string{"127.0.0.1:10092", "127.0.0.1:10093", "127.0.0.1:10094"}
var clusterZk = []string{"localhost:22181", "localhost:22182"}
var singleZk = []string{"localhost:2181"}
var singleBroker = []string{"localhost:9092"}

type BothCloser interface {
	CloseBoth() error
}

func safeCloseBoth(bc BothCloser) {
	if err := bc.CloseBoth(); err != nil {
		panic(err)
	}
}

func safeClose(p io.Closer) {
	if err := p.Close(); err != nil {
		panic(err)
	}
}

func testProd(kp *SyncWriter, wg *sync.WaitGroup, t *testing.T) {
	defer wg.Done()
	defer safeClose(kp)
	for i := 0; i < 10; i++ {
		n, err := kp.Write([]byte{1, 2, 3, 4, 5})
		if n != 5 {
			t.Fatalf("wrote %d bytes in write #%d?", n, i)
		}

		if err != nil {
			t.Fatal(err)
		}
	}

	t.Logf("test for producer %s done", kp.id)
}

func ExampleClient_SyncWriter() {
	kc, _ := NewClient("clientId", []string{"localhost:9092"}, nil)
	kp, _ := kc.NewSyncWriter("some-topic", nil)

	kp.Write([]byte("data"))

	kp2, _ := kc.NewSyncWriter("another-topic", nil)

	n, err := kp2.Write([]byte{1, 2, 3, 4})
	if err != nil {
		panic(err)
	}
	fmt.Println("Wrote", n, "bytes")
}

func TestSyncWriterTwoIWriter(t *testing.T) {
	// defer LogToStderr()()

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
	defer safeClose(kc)
	kp, err := kc.NewSyncWriter("test-topic", nil)
	if err != nil {
		t.Fatal(err)
	}

	kp2, err := kc.NewSyncWriter("test-topic", nil)
	if err != nil {
		t.Fatal(err)
	}

	wg := new(sync.WaitGroup)
	wg.Add(2)
	go testProd(kp, wg, t)
	go testProd(kp2, wg, t)
	wg.Wait()
	if !kp.Closed() || !kp2.Closed() {
		t.Errorf("Either kp (%t) or kp2 (%t) wasn't closed", kp.Closed(), kp2.Closed())
	}
}

func TestSyncWriterOneWriter(t *testing.T) {
	// defer LogToStderr()()
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

	kp, err := NewSyncWriter("testid2", "test-topic", []string{mb1.Addr()}, nil, nil)
	if err != nil {
		panic(err)
	}
	defer safeCloseBoth(kp)

	for i := 0; i < 10; i++ {
		n, err := kp.Write([]byte{1, 2, 3, 4})
		if n != 4 {
			t.Errorf("wrote %d bytes in write #%d?", n, i)
		}

		if err != nil {
			panic(err)
		}
	}
}

func BenchmarkKafkaProdNoCompressionCluster(b *testing.B) {
	// defer LogToStderr()()
	kc, err := NewClient("no-compr-benchmark-c", clusterBroker, nil)
	if err != nil {
		panic(err)
	}
	conf := NewProducerConfig()
	kp, err := kc.NewSyncWriter("no-compr-prod-bench-c", conf)
	testMsg := make([]byte, 255)
	for i := range testMsg {
		testMsg[i] = byte(i)
	}
	defer safeCloseBoth(kp)
	b.Log("Client created")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		kp.Write(testMsg)
	}
}

func BenchmarkKafkaProdNoCompressionSingle(b *testing.B) {
	// defer LogToStderr()()
	kc, err := NewClient("no-compr-benchmark", []string{"127.0.0.1:9092"}, nil)
	if err != nil {
		panic(err)
	}
	conf := NewProducerConfig()
	kp, err := kc.NewSyncWriter("no-compr-prod-bench", conf)
	testMsg := make([]byte, 255)
	for i := range testMsg {
		testMsg[i] = byte(i)
	}
	defer safeCloseBoth(kp)
	b.Log("Client created")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		kp.Write(testMsg)
	}
}
