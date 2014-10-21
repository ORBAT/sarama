package sarama

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"sync"
	"syscall"
)

// BUG(ORBAT): QueuingWriter seems to deadlock the producer if more than ChannelBufferSize concurrent Write()s are done. This can be,
// at least temporarily, handled by using CountingSemaphore

// BUG(ORBAT): QueuingWriter's Close() doesn't wait for pending Write() calls to finish.

// QueuingWriter is an io.Writer that writes messages to Kafka. Parallel calls to Write() will cause messages to be queued by the producer, and
// each Write() call will block until a response is received. The number of concurrent writes is limited by the ChannelBufferSize of the underlying
// Producer (see ProducerConfig for more details.)
type QueuingWriter struct {
	kp       *Producer
	id       string
	topic    string
	closedCh chan struct{}
	log      *log.Logger
	// if CloseClient is true, the client will be closed when Close() is called, effectively turning Close() into CloseAll()
	CloseClient bool
	// mut is the mutex for chanForMsg
	mut        sync.RWMutex
	chanForMsg map[*MessageToSend]chan *ProduceError
	// sem limits the number of concurrent calls to Write()
	sem *CountingSemaphore
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
		id:         id,
		topic:      topic,
		log:        pl,
		closedCh:   make(chan struct{}),
		chanForMsg: make(map[*MessageToSend]chan *ProduceError),
		sem:        NewCountingSemaphore(kp.config.ChannelBufferSize - 1)}

	go func(p *QueuingWriter) {
		errCh := p.kp.Errors()
		p.log.Println("Starting response listener")
		for {
			select {
			case <-p.closedCh:
				p.log.Println("Closing response listener")
				// TODO(ORBAT): handle all inflight Write()s in the event of a close
				return
			case perr, ok := <-errCh:
				if !ok {
					pl.Println("Errors() channel closed?!")
					close(p.closedCh)
					return
				}
				if perr.Err != nil {
					p.log.Println("Got error from Kafka:", err)
				}
				// p.log.Printf("Received %p", perr.Msg)
				p.mut.RLock()
				receiver, ok := p.chanForMsg[perr.Msg]
				p.mut.RUnlock()
				if !ok {
					p.log.Panicf("Nobody wanted *MessageToSend %p?", perr.Msg)
				} else {
					receiver <- perr
				}
				// allow other goroutines to run
				// runtime.Gosched()
			}
		}
	}(p)

	return
}

// Write will queue p as a single message, blocking until a response is received.
// It is safe to call Write in parallel. The number of inflight parallel calls to Write is
// limited by the underlying Producer's ChannelBufferSize; if it is 0, QueuingWriter will
// in effect function synchronously.
//
// n will always be len(p) if the message was sent successfully, 0 otherwise.
func (qw *QueuingWriter) Write(p []byte) (n int, err error) {
	n = len(p)
	qw.sem.Acquire()
	defer qw.sem.Release()
	// counter := qw.sem.Count()
	msg := &MessageToSend{Topic: qw.topic, Key: nil, Value: ByteEncoder(p)}
	responseCh := make(chan *ProduceError, 1)

	defer func() {
		qw.mut.Lock()
		close(responseCh)
		delete(qw.chanForMsg, msg)
		qw.mut.Unlock()
	}()

	qw.mut.Lock()
	qw.chanForMsg[msg] = responseCh
	qw.mut.Unlock()
	qw.kp.Input() <- msg

	select {
	case resp := <-responseCh:
		if resp.Err != nil {
			err = resp.Err
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

func (qw *QueuingWriter) Closed() (closed bool) {
	select {
	case _, ok := <-qw.closedCh:
		closed = !ok
	default:
	}
	return
}

func (qw *QueuingWriter) CloseAll() (err error) {
	if qw.Closed() {
		return syscall.EINVAL
	}

	defer close(qw.closedCh)
	var me *MultiError
	if perr := qw.kp.Close(); perr != nil {
		me = &MultiError{Errors: append(make([]error, 0, 2), perr)}
	}

	qw.log.Println("Closing client")

	if clerr := qw.kp.client.Close(); clerr != nil {
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

func (qw *QueuingWriter) Close() error {
	if qw.Closed() {
		return syscall.EINVAL
	}

	qw.log.Printf("Closing producer. CloseClient = %t", qw.CloseClient)
	if qw.CloseClient == true {
		return qw.CloseAll()
	}
	// TODO(ORBAT): handle all inflight Write()s in the event of a close
	defer close(qw.closedCh)
	return qw.kp.Close()
}
