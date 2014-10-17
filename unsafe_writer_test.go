package sarama

import (
	"fmt"

	"sync"
	"testing"
)

func ExampleClient_UnsafeWriter() {
	kc, _ := NewClient("clientId", []string{"localhost:9092"}, nil)
	kp, _ := kc.NewUnsafeWriter("some-topic", nil)

	kp.Write([]byte("data"))

	kp2, _ := kc.NewUnsafeWriter("another-topic", nil)

	n, err := kp2.Write([]byte{1, 2, 3, 4})
	if err != nil {
		panic(err)
	}
	fmt.Println("Wrote", n, "bytes")
}

func testUnsafeProd(kp *UnsafeWriter, wg *sync.WaitGroup, t *testing.T) {
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

func TestUnsafeWriterTwoInstances(t *testing.T) {
	defer LogToStderr()()
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
	kp, err := kc.NewUnsafeWriter("test-topic", nil)
	if err != nil {
		t.Fatal(err)
	}

	kp2, err := kc.NewUnsafeWriter("test-topic", nil)
	if err != nil {
		t.Fatal(err)
	}

	wg := new(sync.WaitGroup)
	wg.Add(2)
	go testUnsafeProd(kp, wg, t)
	go testUnsafeProd(kp2, wg, t)
	wg.Wait()

	if !kp.Closed() || !kp2.Closed() {
		t.Errorf("Either kp (%t) or kp2 (%t) wasn't closed", kp.Closed(), kp2.Closed())
	}
}

func TestUnsafeWriterOneInstance(t *testing.T) {
	defer LogToStderr()()
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

	kp, err := NewUnsafeWriter("testid2", "test-topic", []string{mb1.Addr()}, nil, nil)
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

func BenchmarkKafkaUnsafeProdNoCompressionCluster(b *testing.B) {
	// defer LogToStderr()()
	kc, err := NewClient("no-compr-benchmark-c", clusterBroker, nil)
	if err != nil {
		panic(err)
	}
	conf := NewProducerConfig()
	kp, err := kc.NewUnsafeWriter("no-compr-prod-bench-c", conf)
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

func BenchmarkKafkaUnsafeProdNoCompressionSingle(b *testing.B) {
	// defer LogToStderr()()
	kc, err := NewClient("no-compr-benchmark", []string{"127.0.0.1:9092"}, nil)
	if err != nil {
		panic(err)
	}
	conf := NewProducerConfig()
	kp, err := kc.NewUnsafeWriter("no-compr-prod-bench", conf)
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
