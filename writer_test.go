package sarama

import (
	"fmt"
	"io"
	"testing"
)

type allCloser interface {
	CloseAll() error
}

type wrAllCloser interface {
	allCloser
	io.Writer
}

func closeAllOrPanic(bc allCloser) {
	if err := bc.CloseAll(); err != nil {
		panic(err)
	}
}

func closeOrPanic(p io.Closer) {
	if err := p.Close(); err != nil {
		panic(err)
	}
}

func newConfig() *ProducerConfig {
	config := NewProducerConfig()
	config.FlushMsgCount = TestBatchSize
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

func testWriter(w wrAllCloser, t *testing.T) {
	bs := []byte(TestMessage)
	bsl := len(bs)
	defer closeAllOrPanic(w)
	for i := 0; i < TestBatchSize; i++ {
		n, err := w.Write(bs)
		if n != bsl {
			t.Errorf("Expected to write %d bytes, wrote %d", bsl, n)
		}
		if err != nil {
			t.Error("Write error:", err.Error())
		}
	}
}
