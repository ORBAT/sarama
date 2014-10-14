package sarama

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"syscall"
)

// SyncWriter is an io.Writer that writes messages to Kafka. Writes are done synchronously and every written []byte is
// one Kafka message.
//
// Close() must be called when the producer is no longer needed.
type SyncWriter struct {
	kp       *Producer
	id       string
	topic    string
	kc       *Client
	closed   bool
	closedCh chan struct{}
	log      *log.Logger
	// if CloseClient is true, the client will be closed when Close() is called, effectively turning Close() into CloseBoth()
	CloseClient bool
}

// NewSyncWriter returns a new SyncWriter.
// If config is nil, sarama default ProducerConfig will be used.
func (c *Client) NewSyncWriter(topic string, config *ProducerConfig) (p *SyncWriter, err error) {
	id := "blocking_prod" + TimestampRandom()
	pl := NewLogger(fmt.Sprintf("BlockPR %s -> %s", id, topic), nil)

	pl.Println("Creating producer")

	kp, err := NewProducer(c, config)

	if err != nil {
		return nil, err
	}

	p = &SyncWriter{kp: kp, id: id, topic: topic, kc: c, log: pl, closedCh: make(chan struct{})}
	return
}

// NewSyncWriter returns a new SyncWriter.

// If either of the configs is nil, sarama default configuration will be used for the omitted config.
func NewSyncWriter(clientId, topic string, brokers []string, pConfig *ProducerConfig, cConfig *ClientConfig) (p *SyncWriter, err error) {
	kc, err := NewClient(clientId, brokers, cConfig)
	if err != nil {
		return
	}

	p, err = kc.NewSyncWriter(topic, pConfig)
	return
}

// ReadFrom reads all available bytes from r and writes them to Kafka. Implements io.ReaderFrom.
//
// Note that SyncWriter doesn't support "streaming", so r is read in full before it's sent.
func (k *SyncWriter) ReadFrom(r io.Reader) (n int64, err error) {
	bs, err := ioutil.ReadAll(r)
	if err != nil {
		return 0, err
	}

	n = int64(len(bs))

	err = k.kp.SendMessage(k.topic, nil, ByteEncoder(bs))
	if err != nil {
		n = 0
	}

	return
}

// Write writes byte slices to Kafka, blocking until the write has been acknowledged (see ProducerConfig's RequiredAcks field.)
// Each written slice is sent out as a single message.
//
// n is len(p) if the send succeeds and 0 in case of errors.
func (k *SyncWriter) Write(p []byte) (n int, err error) {
	n = len(p)
	err = k.kp.SendMessage(k.topic, nil, ByteEncoder(p))

	if err != nil {
		n = 0
	}
	return
}

// Client returns the client used by the SyncWriter.
func (k *SyncWriter) Client() *Client {
	return k.kc
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

// CloseBoth closes the SyncWriter and the client. Returns syscall.EINVAL if the producer is already closed, and an error of type *MultiError
func (k *SyncWriter) CloseBoth() (err error) {
	if k.Closed() {
		return syscall.EINVAL
	}

	defer close(k.closedCh)
	var me *MultiError
	if perr := k.kp.Close(); perr != nil {
		me = &MultiError{Errors: append(make([]error, 0, 2), perr)}
	}

	k.log.Println("Closing client")

	if clerr := k.kc.Close(); clerr != nil {
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

// Close closes the SyncWriter. If k.CloseClient is true, the client will be closed as well (basically turning Close() into CloseBoth().)
// If the SyncWriter has already been closed, Close() will return syscall.EINVAL.
func (k *SyncWriter) Close() error {
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
