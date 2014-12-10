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

type work struct {
	resultCh chan error // channel to return result of write on
	*MessageToSend
}

// QueuingWriter is an io.Writer that writes messages to Kafka. Parallel calls to Write() will cause messages to be queued by the producer, and
// each Write() call will block until a response is received.
type QueuingWriter struct {
	kp          *Producer
	id          string
	topic       string
	stopCh      chan struct{}
	closed      int32 // nonzero if the writer has been closed
	log         StdLogger
	CloseClient bool         // if CloseClient is true, the client will be closed when Close() is called, effectively turning Close() into CloseAll()
	mut         sync.RWMutex // mutex for errChForMsg
	closeMut    sync.Mutex   // mutex for Close and CloseAll
	errChForMsg map[*MessageToSend]chan error
	pendingWg   sync.WaitGroup // WaitGroup for pending message responses
	workCh      chan work      // channel for sending writes to event loop
}

// DefaultChanBufferSize is the default ChannelBufferSize to use when NewQueuingWriter is called with a nil config.
var DefaultChanBufferSize = 20

// NewQueuingWriter creates a new Producer for the client and returns a QueuingWriter for that Producer. If a nil *ProducerConfig is given, default configuration will be used
// and ChannelBufferSize will be set to DefaultChanBufferSize.
func (c *Client) NewQueuingWriter(topic string, config *ProducerConfig) (p *QueuingWriter, err error) {

	if config == nil {
		config = NewProducerConfig()
		config.ChannelBufferSize = DefaultChanBufferSize
	}

	config.AckSuccesses = true

	kp, err := NewProducer(c, config)
	if err != nil {
		return nil, err
	}
	id := "qw-" + TimestampRandom()
	pl := NewLogger(fmt.Sprintf("QueuingWr %s -> %s", id, topic), nil)
	p = &QueuingWriter{kp: kp,
		id:          id,
		topic:       topic,
		log:         pl,
		stopCh:      make(chan struct{}),
		errChForMsg: make(map[*MessageToSend]chan error),
		workCh:      make(chan work, config.ChannelBufferSize)}

	go func() {
		errCh := p.kp.Errors()
		succCh := p.kp.Successes()

		p.log.Println("Starting event loop")
		for {
			select {
			case <-p.stopCh:
				p.log.Println("Stopping event loop")
				return
			case work := <-p.workCh:
				p.errChForMsg[work.MessageToSend] = work.resultCh
				go func() {
					p.kp.Input() <- work.MessageToSend
				}()
			case perr, ok := <-errCh:
				if !ok {
					p.log.Println("Errors() channel closed?!")
					close(p.stopCh)
					continue
				}
				p.sendProdResponse(perr.Msg, perr.Err)
			case succ, ok := <-succCh:
				if !ok {
					p.log.Println("Successes() channel closed?!")
					close(p.stopCh)
					continue
				}
				// p.log.Printf("Received %p", succ)
				p.sendProdResponse(succ, nil)
			}
		}
	}()

	return
}

func (qw *QueuingWriter) sendProdResponse(msg *MessageToSend, perr error) {
	receiver, ok := qw.errChForMsg[msg]
	if !ok {
		qw.log.Panicf("Nobody wanted *MessageToSend %p (%#v)? Map is %#v", msg, msg, qw.errChForMsg)
	} else {
		receiver <- perr
		delete(qw.errChForMsg, msg)
	}
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
	wrk := work{MessageToSend: &MessageToSend{Topic: qw.topic, Key: nil, Value: ByteEncoder(p)}, resultCh: resCh}
	qw.pendingWg.Add(1)
	defer qw.pendingWg.Done()
	qw.workCh <- wrk

	select {
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
func (qw *QueuingWriter) ReadFrom(r io.Reader) (n int64, err error) {
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
	close(qw.stopCh)    // signal response listener stop

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
