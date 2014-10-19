package sarama

import (
	"errors"
	"fmt"
	"log"
	"runtime"
	"sync"
	"syscall"
)

// BUG(ORBAT): QueuingWriter seems to deadlock the producer if more than ChannelBufferSize concurrent Write()s are done. This can be,
// at least temporarily, handled by using a chan with a buffer of ChannelBufferSize-1 as a concurrency limiter.

// QueuingWriter is an io.Writer that writes messages to Kafka. Parallel calls to Write() will cause messages to be queued by the producer.
// Each Write() call will block until a response is received. The number of concurrent writes is limited by ChannelBufferSize
type QueuingWriter struct {
	kp       *Producer
	id       string
	topic    string
	closedCh chan struct{}
	log      *log.Logger
	// if CloseClient is true, the client will be closed when Close() is called, effectively turning Close() into CloseBoth()
	CloseClient bool

	// latestMsg is the latest response received from the broker
	latestMsg *ProduceError
	// latestMut is the mutex for latestMsg
	latestMut sync.Mutex
	// recvCond is the condition Write() waits on
	recvCond *sync.Cond
	// sem limits the number of concurrent calls to ChannelBufferSize-1
	sem *CountingSemaphore
}

func (kp *Producer) NewQueuingWriter(topic string) (p *QueuingWriter, err error) {
	if !kp.config.AckSuccesses {
		return nil, errors.New("AckSuccesses was false")
	}
	id := "queuingw-" + TimestampRandom()
	pl := NewLogger(fmt.Sprintf("QueuingWr %s -> %s", id, topic), nil)
	p = &QueuingWriter{kp: kp, id: id, topic: topic, log: pl, closedCh: make(chan struct{}), sem: NewCountingSemaphore(kp.config.ChannelBufferSize - 1)}
	p.recvCond = sync.NewCond(&p.latestMut)

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

				p.latestMut.Lock()
				p.latestMsg = perr
				p.recvCond.Broadcast()
				p.latestMut.Unlock()
				// allow other goroutines to run so this one doesn't trample p.latestMsg
				runtime.Gosched()
			}
		}
	}(p)

	return
}

func (c *Client) NewQueuingWriter(topic string, config *ProducerConfig) (p *QueuingWriter, err error) {
	if config == nil {
		config = NewProducerConfig()
	}

	config.ChannelBufferSize = 10
	config.AckSuccesses = true

	kp, err := NewProducer(c, config)
	if err != nil {
		return nil, err
	}

	return kp.NewQueuingWriter(topic)
}

var sequentialInts chan int = make(chan int, 20)

func init() {
	i := 0
	go func() {
		for {
			sequentialInts <- i
			i++
		}
	}()
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
	// counter := <-sequentialInts
	msg := &MessageToSend{Topic: qw.topic, Key: nil, Value: ByteEncoder(p)}

	qw.kp.Input() <- msg

	qw.latestMut.Lock()

	// wait until latestMsg contains the message we sent
	for qw.latestMsg == nil || qw.latestMsg.Msg != msg {
		qw.recvCond.Wait()
	}
	defer qw.latestMut.Unlock()

	if qw.latestMsg.Err != nil {
		err = qw.latestMsg.Err
		n = 0
	}
	qw.latestMsg = nil
	return
}

func (qw *QueuingWriter) Closed() (closed bool) {
	select {
	case _, ok := <-qw.closedCh:
		closed = !ok
	default:
	}
	return
}

func (qw *QueuingWriter) CloseBoth() (err error) {
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
	// TODO(ORBAT): handle all inflight Write()s in the event of a close
	qw.log.Printf("Closing producer. CloseClient = %t", qw.CloseClient)
	if qw.CloseClient == true {
		return qw.CloseBoth()
	}
	defer close(qw.closedCh)
	return qw.kp.Close()
}
