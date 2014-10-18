package sarama

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"syscall"
)

// UnsafeWriter is an io.Writer that writes messages to Kafka, ignoring error responses sent by the brokers.
//
// Close() must be called when the writer is no longer needed.
type UnsafeWriter struct {
	kp       *Producer
	id       string
	topic    string
	closed   bool
	closedCh chan struct{}
	log      *log.Logger
	// if CloseClient is true, the client will be closed when Close() is called, effectively turning Close() into CloseBoth()
	CloseClient bool
}

// NewUnsafeWriter returns a new UnsafeWriter.
// If config is nil, sarama default ProducerConfig will be used.
func (c *Client) NewUnsafeWriter(topic string, config *ProducerConfig) (p *UnsafeWriter, err error) {
	id := "unsafew-" + TimestampRandom()
	pl := NewLogger(fmt.Sprintf("UnsafeWr %s -> %s", id, topic), nil)

	closedCh := make(chan struct{})

	pl.Println("Creating producer")

	kp, err := NewProducer(c, config)

	if err != nil {
		return nil, err
	}

	go func(errCh chan error, closedCh chan struct{}) {
		pl.Println("Starting error listener")
		for {
			select {
			case <-closedCh:
				pl.Println("Closing error listener")
				return
			case err, ok := <-kp.Errors():
				if !ok {
					pl.Println("Errors() channel closed?!")
					return
				}
				if err != nil {
					pl.Println("Got error from Kafka:", err)
				}
			}
		}
	}(kp.Errors(), closedCh)

	p = &UnsafeWriter{kp: kp, id: id, topic: topic, log: pl, closedCh: closedCh}
	return
}

// NewUnsafeWriter returns a new UnsafeWriter.

// If either of the configs is nil, sarama default configuration will be used for the omitted config.
func NewUnsafeWriter(clientId, topic string, brokers []string, pConfig *ProducerConfig, cConfig *ClientConfig) (p *UnsafeWriter, err error) {
	kc, err := NewClient(clientId, brokers, cConfig)
	if err != nil {
		return
	}

	p, err = kc.NewUnsafeWriter(topic, pConfig)
	return
}

// ReadFrom reads all available bytes from r and writes them to Kafka. Implements io.ReaderFrom.
//
// Note that UnsafeWriter doesn't support "streaming", so r is read in full before it's sent.
func (k *UnsafeWriter) ReadFrom(r io.Reader) (n int64, err error) {
	bs, err := ioutil.ReadAll(r)
	if err != nil {
		return 0, err
	}

	n = int64(len(bs))

	err = k.kp.QueueMessage(k.topic, nil, ByteEncoder(bs))
	if err != nil {
		n = 0
	}

	return
}

// Write writes byte slices to Kafka, blocking until the write has been acknowledged (see ProducerConfig's RequiredAcks field.)
// Each written slice is sent out as a single message.
//
// n is len(p) if the send succeeds and 0 in case of errors.
func (k *UnsafeWriter) Write(p []byte) (n int, err error) {
	n = len(p)

	err = k.kp.QueueMessage(k.topic, nil, ByteEncoder(p))

	if err != nil {
		n = 0
	}
	return
}

// Client returns the client used by the UnsafeWriter.
func (k *UnsafeWriter) Client() *Client {
	return k.kp.client
}

// Closed returns true if the UnsafeWriter has been closed, false otherwise. Thread-safe.
func (k *UnsafeWriter) Closed() (closed bool) {
	select {
	case _, ok := <-k.closedCh:
		closed = !ok
	default:
	}
	return
}

// CloseBoth closes the UnsafeWriter and the client. Returns syscall.EINVAL if the producer is already closed, and an error of type *MultiError if one or both of
// Producer#Close() and Client#Close() returns an error.
func (k *UnsafeWriter) CloseBoth() (err error) {
	if k.Closed() {
		return syscall.EINVAL
	}

	defer close(k.closedCh)
	var me *MultiError
	if perr := k.kp.Close(); perr != nil {
		me = &MultiError{Errors: append(make([]error, 0, 2), perr)}
	}

	k.log.Println("Closing client")

	if clerr := k.kp.client.Close(); clerr != nil {
		if me == nil {
			me = &MultiError{Errors: make([]error, 0, 1)}
		}
		me.Errors = append(me.Errors, clerr)
	}

	if me != nil {
		err = me
	}

	return
}

// SetLogger sets the logger used by this UnsafeWriter.
func (k *UnsafeWriter) SetLogger(l *log.Logger) {
	k.log = l
}

// CloseWait blocks until the UnsafeWriter is closed.
func (k *UnsafeWriter) CloseWait() {
	<-k.closedCh
}

// Close closes the UnsafeWriter. If k.CloseClient is true, the client will be closed as well (basically turning Close() into CloseBoth().)
// If the UnsafeWriter has already been closed, Close() will return syscall.EINVAL.
func (k *UnsafeWriter) Close() error {
	if k.Closed() {
		return syscall.EINVAL
	}

	k.log.Printf("Closing producer. CloseClient = %t", k.CloseClient)
	if k.CloseClient == true {
		return k.CloseBoth()
	}
	defer close(k.closedCh)
	return k.kp.Close()
}
