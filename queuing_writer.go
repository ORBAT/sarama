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

// QueuingWriter is an io.Writer that writes messages to Kafka. Parallel calls to Write() will cause messages to be queued by the producer, and
// each Write() call will block until a response is received.
type QueuingWriter struct {
	kp          *Producer
	id          string
	topic       string
	closed      int32 // nonzero if the writer has been closed
	log         *log.Logger
	CloseClient bool           // if CloseClient is true, the client will be closed when Close() is called, effectively turning Close() into CloseAll()
	closeMut    sync.Mutex     // mutex for Close and CloseAll
	pendingWg   sync.WaitGroup // WaitGroup for pending message responses
}

// NewQueuingWriter creates a new Producer for the client and returns a QueuingWriter for that Producer. If a nil *ProducerConfig is given, NewProducerConfig() will be used.
func (c *Client) NewQueuingWriter(topic string, config *ProducerConfig) (p *QueuingWriter, err error) {

	if config == nil {
		config = NewProducerConfig()
	}

	kp, err := NewProducer(c, config)
	if err != nil {
		return nil, err
	}

	id := "qw-" + TimestampRandom()
	pl := NewLogger(fmt.Sprintf("QueuingWr %s -> %s", id, topic), nil)
	p = &QueuingWriter{kp: kp,
		id:    id,
		topic: topic,
		log:   pl}
	return
}

// Write will queue p as a single message, blocking until a response is received.
// It is safe to call Write in parallel.
//
// n will always be len(p) if the message was sent successfully, 0 otherwise.
//
// TODO(ORBAT): QueuingWriter Write() timeout
func (qw *QueuingWriter) Write(p []byte) (n int, err error) {
	n = len(p)
	resCh := make(chan error, 1)

	msg := &MessageToSend{Topic: qw.topic, Key: nil, Value: ByteEncoder(p), Promise: resCh}

	qw.pendingWg.Add(1)
	defer qw.pendingWg.Done()
	qw.kp.Input() <- msg

	select {
	case perr := <-qw.kp.Errors(): // note: temporary until Producer API is frozen
		close(resCh) // might cause runtime to hurl with send on closed channel but oh well
		err = perr.Err
		n = 0
	case resp := <-resCh:
		close(resCh)
		if resp != nil {
			err = resp
			n = 0
		}
	}

	return
}

// ReadFrom reads all available bytes from r and writes them to Kafka in a single message. Implements io.ReaderFrom.
func (qw *QueuingWriter) ReadFrom(r io.Reader) (int64, error) {
	p, err := ioutil.ReadAll(r)
	if err != nil {
		return 0, err
	}

	ni, err := qw.Write(p)
	return int64(ni), err
}

// Closed returns true if the QueuingWriter has been closed, false otherwise. Thread-safe.
func (qw *QueuingWriter) Closed() bool {
	return atomic.LoadInt32(&qw.closed) != 0
}

// SetLogger sets the logger used by this QueuingWriter.
func (qw *QueuingWriter) SetLogger(l *log.Logger) {
	qw.log = l
}

// CloseAll closes both the underlying Producer and Client. If the writer has already been closed, CloseAll will
// return syscall.EINVAL.
func (qw *QueuingWriter) CloseAll() (err error) {
	qw.CloseClient = true
	return qw.Close()
}

// Close closes the writer and its underlying producer. If CloseClient is true,
// the client will be closed as well. If the writer has already been closed, Close will
// return syscall.EINVAL.
func (qw *QueuingWriter) Close() (err error) {
	qw.log.Println("Close() called", qw.CloseClient)
	qw.closeMut.Lock()
	defer qw.closeMut.Unlock()

	qw.log.Println("Close() mutex acquired")

	if qw.Closed() {
		return syscall.EINVAL
	}

	atomic.StoreInt32(&qw.closed, 1)
	qw.pendingWg.Wait() // wait for pending writes to complete

	var me *MultiError

	qw.log.Println("Closing producer")

	if perr := qw.kp.Close(); perr != nil {
		me = &MultiError{Errors: append(make([]error, 0, 2), perr)}
	}

	if qw.CloseClient {
		qw.log.Println("Closing client")
		if clerr := qw.kp.client.Close(); clerr != nil {
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
