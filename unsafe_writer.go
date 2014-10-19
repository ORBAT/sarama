package sarama

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"syscall"
)

// BUG(ORBAT): None of the Writers handles Close() gracefully.

// BUG(ORBAT): UnsafeWriter seems to deadlock the producer if more than ChannelBufferSize concurrent Write()s are done. This can be,
// at least temporarily, handled by using CountingSemaphore

// UnsafeWriter is an io.Writer that writes messages to Kafka, ignoring any error responses sent by the brokers. Parallel calls to Write are safe,
// but they are limited by the underlying Producer's ChannelBufferSize.
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
	// sem limits the number of concurrent calls to ChannelBufferSize-1
	sem *CountingSemaphore
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

	go func(errCh <-chan *ProduceError, closedCh chan struct{}, pl *log.Logger) {
		pl.Println("Starting error listener")
		for {
			select {
			case <-closedCh:
				pl.Println("Closing error listener")
				return
			case perr, ok := <-errCh:
				if !ok {
					pl.Println("Errors() channel closed?!")
					return
				}
				if perr.Err != nil {
					pl.Println("Got error from Kafka:", err)
				}
			}
		}
	}(kp.Errors(), closedCh, pl)

	p = &UnsafeWriter{kp: kp, id: id, topic: topic, log: pl, closedCh: closedCh, sem: NewCountingSemaphore(kp.config.ChannelBufferSize - 1)}
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

// ReadFrom reads all available bytes from r and writes them to Kafka without checking for broker error responses. The returned
// error will be either nil or anything returned when reading from r. The returned int64 will always be the total length of bytes read from r,
// or 0 if reading from r returned an error. Implements io.ReaderFrom.
//
// Note that UnsafeWriter doesn't support "streaming", so r is read in full before it's sent.
func (k *UnsafeWriter) ReadFrom(r io.Reader) (int64, error) {
	bs, err := ioutil.ReadAll(r)
	if err != nil {
		return 0, err
	}
	ni, _ := k.Write(bs)
	return int64(ni), nil
}

// Write writes byte slices to Kafka without checking for error responses. n will always be len(p) and err will be nil. The maximum number of parallel
// Write calls is limited by the Producer's ChannelBufferSize
func (k *UnsafeWriter) Write(p []byte) (n int, err error) {
	k.sem.Acquire()
	defer k.sem.Release()
	n = len(p)
	k.kp.Input() <- &MessageToSend{Topic: k.topic, Key: nil, Value: ByteEncoder(p)}

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
