package sarama

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"sync"
	"sync/atomic"
	"syscall"
)

// UnsafeWriter is an io.Writer that writes messages to Kafka, ignoring any error responses sent by the brokers. Parallel calls to Write are safe.
//
// Close() must be called when the writer is no longer needed.
type UnsafeWriter struct {
	kp       *Producer
	id       string
	topic    string
	closed   int32
	closedCh chan struct{}
	log      *log.Logger
	closeMut sync.Mutex // mutex for Close and CloseAll
	// if CloseClient is true, the client will be closed when Close() is called, effectively turning Close() into CloseAll()
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

	p = &UnsafeWriter{kp: kp, id: id, topic: topic, log: pl, closedCh: closedCh}

	go func() {
		pl.Println("Starting error listener")
		for {
			select {
			case <-closedCh:
				pl.Println("Closing error listener")
				return
			case perr, ok := <-kp.Errors():
				if !ok {
					pl.Println("Errors() channel closed?!")
					p.Close()
					return
				}
				pl.Println("Got error from Kafka:", perr)
			}
		}
	}()

	return
}

// NewUnsafeWriter returns a new UnsafeWriter. If either of the configs is nil, sarama default configuration will be used for the omitted config.
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
func (uw *UnsafeWriter) ReadFrom(r io.Reader) (int64, error) {
	bs, err := ioutil.ReadAll(r)
	if err != nil {
		return 0, err
	}
	ni, _ := uw.Write(bs)
	return int64(ni), nil
}

// Write writes byte slices to Kafka without checking for error responses. n will always be len(p) and err will be nil.
func (uw *UnsafeWriter) Write(p []byte) (n int, err error) {
	n = len(p)

	uw.kp.Input() <- &MessageToSend{Topic: uw.topic, Key: nil, Value: ByteEncoder(p)}

	return
}

// Closed returns true if the UnsafeWriter has been closed, false otherwise. Thread-safe.
func (uw *UnsafeWriter) Closed() bool {
	return atomic.LoadInt32(&uw.closed) != 0
}

// SetLogger sets the logger used by this UnsafeWriter.
func (uw *UnsafeWriter) SetLogger(l *log.Logger) {
	uw.log = l
}

// CloseAll closes both the underlying Producer and Client. If the writer has already been closed, CloseAll will
// return syscall.EINVAL.
func (uw *UnsafeWriter) CloseAll() (err error) {
	uw.CloseClient = true
	return uw.Close()
}

// Close closes the writer and its underlying producer. If uw.CloseClient is true,
// the client will be closed as well. If the writer has already been closed, Close will
// return syscall.EINVAL.
func (uw *UnsafeWriter) Close() (err error) {
	uw.log.Println("Close() called", uw.CloseClient)
	uw.closeMut.Lock()
	defer uw.closeMut.Unlock()

	uw.log.Println("Close() mutex acquired")

	if uw.Closed() {
		return syscall.EINVAL
	}

	atomic.StoreInt32(&uw.closed, 1)

	close(uw.closedCh)

	var me *MultiError

	uw.log.Println("Closing producer")

	if perr := uw.kp.Close(); perr != nil {
		me = &MultiError{Errors: append(make([]error, 0, 2), perr)}
	}

	if uw.CloseClient {
		uw.log.Println("Closing client")
		if clerr := uw.kp.client.Close(); clerr != nil {
			if me == nil {
				me = &MultiError{Errors: make([]error, 0, 1)}
			}
			me.Errors = append(me.Errors, clerr)
		}
	}

	if me != nil {
		err = me
	}
	return
}
