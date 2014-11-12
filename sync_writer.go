package sarama

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"syscall"
)

// SyncWriter is an io.Writer that writes messages to Kafka. Writes are done synchronously and every written []byte is
// one Kafka message. Calling Write in parallel is safe.
//
// Close() must be called when the producer is no longer needed.
type SyncWriter struct {
	kp       *Producer
	id       string
	topic    string
	closedCh chan struct{}
	log      *log.Logger
	// if CloseClient is true, the client will be closed when Close() is called, effectively turning Close() into CloseAll()
	CloseClient bool
	// sem limits the number of concurrent calls to 1
	sem *CountingSemaphore
}

// NewSyncWriter returns a new SyncWriter.
// If config is nil, sarama default ProducerConfig will be used. AckSuccesses is always set to true and FlushMsgCount to 1.
func (c *Client) NewSyncWriter(topic string, config *ProducerConfig) (p *SyncWriter, err error) {
	id := "syncw-" + TimestampRandom()
	pl := NewLogger(fmt.Sprintf("SyncWr %s -> %s", id, topic), nil)

	pl.Println("Creating producer")

	if config == nil {
		config = NewProducerConfig()

	}

	config.AckSuccesses = true
	config.FlushMsgCount = 1

	kp, err := NewProducer(c, config)

	if err != nil {
		return nil, err
	}

	p = &SyncWriter{kp: kp,
		id:       id,
		topic:    topic,
		log:      pl,
		closedCh: make(chan struct{}),
		sem:      NewCountingSemaphore(1)}
	return
}

// ReadFrom reads all available bytes from r and writes them to Kafka in a single message. Implements io.ReaderFrom.
func (k *SyncWriter) ReadFrom(r io.Reader) (n int64, err error) {
	p, err := ioutil.ReadAll(r)
	if err != nil {
		return 0, err
	}

	ni, err := k.Write(p)
	return int64(ni), err
}

// Write writes byte slices to Kafka, blocking until the write has been acknowledged. Each written slice is sent out as a single message, with no
// message batching. Calling Write in parallel is safe.
//
// n is len(p) if the send succeeds and 0 in case of errors.
func (k *SyncWriter) Write(p []byte) (n int, err error) {
	k.sem.Acquire()
	defer k.sem.Release()

	k.kp.Input() <- &MessageToSend{Topic: k.topic, Key: nil, Value: ByteEncoder(p)}

	select {
	case perr := <-k.kp.Errors():
		n = 0
		err = perr.Err
	case _ = <-k.kp.Successes():
		n = len(p)
	}

	return
}

// Client returns the client used by the SyncWriter.
func (k *SyncWriter) Client() *Client {
	return k.kp.client
}

// Closed returns true if the SyncWriter has been closed, false otherwise. Thread-safe.
func (k *SyncWriter) Closed() (closed bool) {
	select {
	case _, ok := <-k.closedCh:
		closed = !ok
	default:
	}
	return
}

// CloseAll closes the SyncWriter and the client. Returns syscall.EINVAL if the producer is already closed, and an error of type *MultiError if one or both of
// Producer#Close() and Client#Close() returns an error.
func (k *SyncWriter) CloseAll() (err error) {
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

// SetLogger sets the logger used by this SyncWriter.
func (k *SyncWriter) SetLogger(l *log.Logger) {
	k.log = l
}

// CloseWait blocks until the SyncWriter is closed.
func (k *SyncWriter) CloseWait() {
	<-k.closedCh
}

// Close closes the SyncWriter. If k.CloseClient is true, the client will be closed as well (basically turning Close() into CloseAll().)
// If the SyncWriter has already been closed, Close() will return syscall.EINVAL.
func (k *SyncWriter) Close() error {
	if k.Closed() {
		return syscall.EINVAL
	}

	k.log.Printf("Closing producer. CloseClient = %t", k.CloseClient)
	if k.CloseClient == true {
		return k.CloseAll()
	}
	defer close(k.closedCh)
	return k.kp.Close()
}
