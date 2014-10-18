package sarama

import (
	"fmt"
	"log"
	"sync"
	"syscall"
)

// QueuingWriter is an io.Writer that writes messages to Kafka. Parallel calls to Write() will cause messages to be queued by the producer
type QueuingWriter struct {
	kp       *Producer
	id       string
	topic    string
	closedCh chan struct{}
	log      *log.Logger
	// if CloseClient is true, the client will be closed when Close() is called, effectively turning Close() into CloseBoth()
	CloseClient bool
	latestMsg   *ProduceError
	// mutex for latestMsg
	latestMut sync.Mutex
	recvCond  sync.Cond
}

func (c *Client) NewQueuingWriter(topic string, config *ProducerConfig) (p *QueuingWriter, err error) {
	id := "queuingw-" + TimestampRandom()
	pl := NewLogger(fmt.Sprintf("QueuingWr %s -> %s", id, topic), nil)

	pl.Println("Creating producer")

	if config == nil {
		config = NewProducerConfig()

	}

	config.AckSuccesses = true

	kp, err := NewProducer(c, config)
	if err != nil {
		return nil, err
	}

	p = &QueuingWriter{kp: kp, id: id, topic: topic, log: pl, closedCh: make(chan struct{})}

	go func(p *QueuingWriter) {
		errCh := kp.Errors()
		p.log.Println("Starting error listener")
		for {
			select {
			case <-p.closedCh:
				p.log.Println("Closing error listener")
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
				defer p.latestMut.Unlock()
				p.log.Printf("Received response %#v", perr)
				p.latestMsg = perr
				p.recvCond.Broadcast()
			}
		}
	}(p)

	p.recvCond.L = &p.latestMut
	return
}

func (qw *QueuingWriter) Write(p []byte) (n int, err error) {
	n = len(p)
	msg := &MessageToSend{Topic: qw.topic, Key: nil, Value: ByteEncoder(p)}
	qw.kp.Input() <- msg
	qw.latestMut.Lock()

	for qw.latestMsg == nil || qw.latestMsg.Msg != msg {
		qw.recvCond.Wait()
	}
	defer qw.latestMut.Unlock()

	if qw.latestMsg.Err != nil {
		err = qw.latestMsg.Err
		n = 0
	}

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
