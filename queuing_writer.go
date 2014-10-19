package sarama

import (
	"errors"
	"fmt"
	"log"
	"runtime"
	"sync"
	"syscall"
)

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
}

func (kp *Producer) NewQueuingWriter(topic string) (p *QueuingWriter, err error) {
	if !kp.config.AckSuccesses {
		return nil, errors.New("AckSuccesses was false")
	}
	id := "queuingw-" + TimestampRandom()
	pl := NewLogger(fmt.Sprintf("QueuingWr %s -> %s", id, topic), nil)
	p = &QueuingWriter{kp: kp, id: id, topic: topic, log: pl, closedCh: make(chan struct{})}
	p.recvCond = sync.NewCond(&p.latestMut)

	go func(p *QueuingWriter) {
		errCh := p.kp.Errors()
		p.log.Println("Starting response listener")
		for {
			select {
			case <-p.closedCh:
				p.log.Println("Closing response listener")
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

var nums chan int = make(chan int, 20)

func init() {
	i := 0
	go func() {
		for {
			nums <- i
			i++
		}
	}()
}

func (qw *QueuingWriter) Write(p []byte) (n int, err error) {
	n = len(p)
	// counter := <-nums
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

	qw.log.Printf("Closing producer. CloseClient = %t", qw.CloseClient)
	if qw.CloseClient == true {
		return qw.CloseBoth()
	}
	defer close(qw.closedCh)
	return qw.kp.Close()
}
