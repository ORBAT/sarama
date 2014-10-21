package sarama

import "time"

func forceFlushThreshold() int {
	return int(MaxRequestSize - (10 * 1024)) // 10KiB is safety room for misc. overhead, we might want to calculate this more precisely?
}

// ProducerConfig is used to pass multiple configuration options to NewProducer.
type ProducerConfig struct {
	Partitioner       Partitioner      // Chooses the partition to send messages to (defaults to random).
	RequiredAcks      RequiredAcks     // The level of acknowledgement reliability needed from the broker (defaults to WaitForLocal).
	Timeout           time.Duration    // The maximum duration the broker will wait the receipt of the number of RequiredAcks. This is only relevant when RequiredAcks is set to WaitForAll or a number > 1. Only supports millisecond resolution, nanoseconds will be truncated.
	Compression       CompressionCodec // The type of compression to use on messages (defaults to no compression).
	FlushMsgCount     int              // The number of messages needed to trigger a flush.
	FlushFrequency    time.Duration    // If this amount of time elapses without a flush, one will be queued.
	FlushByteCount    int              // If this many bytes of messages are accumulated, a flush will be triggered.
	AckSuccesses      bool             // If enabled, successfully delivered messages will also be returned on the Errors channel, with a nil Err field
	MaxMessageBytes   int              // The maximum permitted size of a message (defaults to 1000000)
	ChannelBufferSize int              // The size of the buffers of the channels between the different goroutines. Defaults to 0 (unbuffered).
}

// NewProducerConfig creates a new ProducerConfig instance with sensible defaults.
func NewProducerConfig() *ProducerConfig {
	return &ProducerConfig{
		Partitioner:     NewRandomPartitioner(),
		RequiredAcks:    WaitForLocal,
		MaxMessageBytes: 1000000,
	}
}

// Validate checks a ProducerConfig instance. It will return a
// ConfigurationError if the specified value doesn't make sense.
func (config *ProducerConfig) Validate() error {
	if config.RequiredAcks < -1 {
		return ConfigurationError("Invalid RequiredAcks")
	}

	if config.Timeout < 0 {
		return ConfigurationError("Invalid Timeout")
	} else if config.Timeout%time.Millisecond != 0 {
		Logger.Println("ProducerConfig.Timeout only supports millisecond resolution; nanoseconds will be truncated.")
	}

	if config.FlushMsgCount < 0 {
		return ConfigurationError("Invalid FlushMsgCount")
	}

	if config.FlushByteCount < 0 {
		return ConfigurationError("Invalid FlushByteCount")
	} else if config.FlushByteCount >= forceFlushThreshold() {
		Logger.Println("ProducerConfig.FlushByteCount too close to MaxRequestSize; it will be ignored.")
	}

	if config.FlushFrequency < 0 {
		return ConfigurationError("Invalid FlushFrequency")
	}

	if config.Partitioner == nil {
		return ConfigurationError("No partitioner set")
	}

	if config.MaxMessageBytes <= 0 {
		return ConfigurationError("Invalid MaxMessageBytes")
	} else if config.MaxMessageBytes >= forceFlushThreshold() {
		Logger.Println("ProducerConfig.MaxMessageBytes too close to MaxRequestSize; it will be ignored.")
	}

	return nil
}

// Producer publishes Kafka messages. It routes messages to the correct broker
// for the provided topic-partition, refreshing metadata as appropriate, and
// parses responses for errors. You must read from the Errors() channel or the
// producer will deadlock. You must call Close() on a producer to avoid
// leaks: it will not be garbage-collected automatically when it passes out of
// scope (this is in addition to calling Close on the underlying client, which
// is still necessary).
type Producer struct {
	client             *Client
	config             ProducerConfig
	errors             chan *ProduceError
	input, readyToSend chan *MessageToSend
	stopper            chan struct{}
}

// NewProducer creates a new Producer using the given client.
func NewProducer(client *Client, config *ProducerConfig) (*Producer, error) {
	// Check that we are not dealing with a closed Client before processing
	// any other arguments
	if client.Closed() {
		return nil, ClosedClient
	}

	if config == nil {
		config = NewProducerConfig()
	}

	if err := config.Validate(); err != nil {
		return nil, err
	}

	p := &Producer{
		client:      client,
		config:      *config,
		errors:      make(chan *ProduceError),
		input:       make(chan *MessageToSend),
		readyToSend: make(chan *MessageToSend),
		stopper:     make(chan struct{}),
	}

	// launch our singleton dispatchers
	go p.topicDispatcher()
	go p.brokerDispatcher()

	return p, nil
}

type flagSet int8

const (
	retried  flagSet = 1 << iota // message has been retried
	chaser                       // message is last in a group that failed
	ref                          // add a reference to a singleton channel
	unref                        // remove a reference from a singleton channel
	shutdown                     // start the shutdown process
)

// MessageToSend is the collection of elements passed to the Producer in order to send a message.
type MessageToSend struct {
	Topic      string
	Key, Value Encoder

	// these are filled in by the producer as the message is processed
	broker    *Broker
	offset    int64
	partition int32
	flags     flagSet
}

// Offset is the offset of the message stored on the broker. This is only guaranteed to be defined if
// the message was successfully delivered and RequiredAcks is not NoResponse.
func (m *MessageToSend) Offset() int64 {
	return m.offset
}

// Partition is the partition that the message was sent to. This is only guaranteed to be defined if
// the message was successfully delivered.
func (m *MessageToSend) Partition() int32 {
	return m.partition
}

func (m *MessageToSend) byteSize() int {
	size := 26 // the metadata overhead of CRC, flags, etc.
	if m.Key != nil {
		size += m.Key.Length()
	}
	if m.Value != nil {
		size += m.Value.Length()
	}
	return size
}

// ProduceError is the type of error generated when the producer fails to deliver a message.
// It contains the original MessageToSend as well as the actual error value. If the AckSuccesses configuration
// value is set to true then every message sent generates a ProduceError, but successes will have a nil Err field.
type ProduceError struct {
	Msg *MessageToSend
	Err error
}

// Errors is the output channel back to the user. You MUST read from this channel or the Producer will deadlock.
// It is suggested that you send messages and read errors together in a single select statement.
func (p *Producer) Errors() <-chan *ProduceError {
	return p.errors
}

// Input is the input channel for the user to write messages to that they wish to send.
func (p *Producer) Input() chan<- *MessageToSend {
	return p.input
}

// Close shuts down the producer and flushes any messages it may have buffered.
// You must call this function before a producer object passes out of scope, as
// it may otherwise leak memory. You must call this before calling Close on the
// underlying client.
func (p *Producer) Close() error {
	p.input <- &MessageToSend{flags: shutdown}
	<-p.stopper
	close(p.input)
	return nil
}

///////////////////////////////////////////
// in normal processing, a message flows through the following functions from top to bottom,
// starting at topicDispatcher (which reads from Producer.input) and ending in flusher
// (which sends the message to the broker)
///////////////////////////////////////////

// singleton
func (p *Producer) topicDispatcher() {
	handlers := make(map[string]chan *MessageToSend)
	refs := 0

	for {
		msg := <-p.input

		if msg == nil {
			Logger.Printf("somebody sent a nil message to the producer, it was ignored")
			continue
		}

		if msg.flags&shutdown != 0 {
			break
		} else if msg.flags&ref != 0 {
			refs++
			continue
		} else if msg.flags&unref != 0 {
			refs--
			continue
		}

		if (p.config.Compression == CompressionNone && msg.Value != nil && msg.Value.Length() > p.config.MaxMessageBytes) ||
			(msg.byteSize() > p.config.MaxMessageBytes) {

			p.errors <- &ProduceError{Msg: msg, Err: MessageSizeTooLarge}
			continue
		}

		handler := handlers[msg.Topic]
		if handler == nil {
			handler = make(chan *MessageToSend, p.config.ChannelBufferSize)
			go p.partitionDispatcher(msg.Topic, handler)
			handlers[msg.Topic] = handler
		}

		handler <- msg
	}

	for _, handler := range handlers {
		close(handler)
	}

	for refs > 0 {
		msg := <-p.input
		if msg.flags&ref != 0 {
			refs++
		} else if msg.flags&unref != 0 {
			refs--
		} else {
			p.errors <- &ProduceError{Msg: msg, Err: ShuttingDown}
		}
	}

	close(p.stopper)
}

// one per topic
func (p *Producer) partitionDispatcher(topic string, input chan *MessageToSend) {
	handlers := make(map[int32]chan *MessageToSend)

	for msg := range input {
		if msg.flags&retried == 0 {
			err := p.assignPartition(msg)
			if err != nil {
				p.errors <- &ProduceError{Msg: msg, Err: err}
				continue
			}
		}

		handler := handlers[msg.partition]
		if handler == nil {
			handler = make(chan *MessageToSend, p.config.ChannelBufferSize)
			go p.leaderDispatcher(msg.Topic, msg.partition, handler)
			handlers[msg.partition] = handler
		}

		handler <- msg
	}

	for _, handler := range handlers {
		close(handler)
	}
}

// one per partition per topic
func (p *Producer) leaderDispatcher(topic string, partition int32, input chan *MessageToSend) {
	var leader *Broker
	var err error

	var retryQueue, backlog []*MessageToSend

	p.readyToSend <- &MessageToSend{flags: ref}

	for msg := range input {
		if msg.flags&retried == 0 {
			// normal case
			if len(retryQueue) > 0 {
				backlog = append(backlog, msg)
				continue
			}

			if leader == nil {
				leader, err = p.client.Leader(topic, partition)
				if err != nil {
					p.errors <- &ProduceError{Msg: msg, Err: err}
					continue
				}
			}

			msg.broker = leader

			// messages from multiple topics/partitions may go to the same broker, so rather than storing a local map
			// of brokers->goroutines, we route everything back through the singleton brokerDispatcher
			p.readyToSend <- msg
		} else if msg.flags&chaser == 0 {
			// retry flag set, chaser flag not set
			if len(retryQueue) == 0 {
				// on the very first retried message we send off a chaser so that we know when everything "in between" has made it
				// back to us and we can safely flush
				p.readyToSend <- &MessageToSend{Topic: topic, partition: partition, broker: leader, flags: chaser}
			}
			retryQueue = append(retryQueue, msg)
		} else {
			// retry *and* chaser flag set, flush everything and return to normal processing
			err = p.client.RefreshTopicMetadata(topic)
			if err == nil {
				leader, err = p.client.Leader(topic, partition)
			}
			if err == nil {
				for _, msg := range retryQueue {
					msg.broker = leader
					p.readyToSend <- msg
				}
				for _, msg := range backlog {
					msg.broker = leader
					p.readyToSend <- msg
				}
			} else {
				p.returnMessages(retryQueue, err)
				p.returnMessages(backlog, err)
			}
			retryQueue = nil
			backlog = nil
		}
	}

	p.readyToSend <- &MessageToSend{flags: unref}
}

// singleton
func (p *Producer) brokerDispatcher() {
	handlers := make(map[*Broker]chan *MessageToSend)
	refs := 0

	for msg := range p.readyToSend {
		if msg.flags&ref == ref {
			refs++
		} else if msg.flags&unref == unref {
			refs--
			if refs == 0 {
				close(p.readyToSend)
			}
		} else {
			handler := handlers[msg.broker]
			if handler == nil {
				handler = make(chan *MessageToSend)
				go p.messageAggregator(msg.broker, handler)
				handlers[msg.broker] = handler
			}

			handler <- msg
		}
	}

	for _, handler := range handlers {
		close(handler)
	}
}

// one per broker
func (p *Producer) messageAggregator(broker *Broker, input chan *MessageToSend) {
	var ticker *time.Ticker
	var timer <-chan time.Time
	if p.config.FlushFrequency > 0 {
		ticker = time.NewTicker(p.config.FlushFrequency)
		timer = ticker.C
	}

	var buffer []*MessageToSend
	var doFlush chan []*MessageToSend
	var bytesAccumulated int

	flusher := make(chan []*MessageToSend)
	go p.flusher(broker, flusher)

	for {
		select {
		case msg := <-input:
			if msg == nil {
				goto shutdown
			}

			if bytesAccumulated+msg.byteSize() >= forceFlushThreshold() {
				flusher <- buffer
				buffer = nil
				doFlush = nil
				bytesAccumulated = 0
			}

			buffer = append(buffer, msg)
			bytesAccumulated += msg.byteSize()

			if len(buffer) >= p.config.FlushMsgCount ||
				(p.config.FlushByteCount > 0 && bytesAccumulated >= p.config.FlushByteCount) {
				doFlush = flusher
			}
		case <-timer:
			doFlush = flusher
		case doFlush <- buffer:
			buffer = nil
			doFlush = nil
			bytesAccumulated = 0
		}
	}

shutdown:
	if ticker != nil {
		ticker.Stop()
	}
	if len(buffer) > 0 {
		flusher <- buffer
	}
	close(flusher)
}

// one per broker
func (p *Producer) flusher(broker *Broker, input chan []*MessageToSend) {
	var closing error
	currentRetries := make(map[string]map[int32]error)

	p.input <- &MessageToSend{flags: ref}
	for batch := range input {
		if closing != nil {
			p.retryMessages(batch, closing)
			continue
		}

		// group messages by topic/partition
		msgSets := make(map[string]map[int32][]*MessageToSend)
		for i, msg := range batch {
			if currentRetries[msg.Topic] != nil && currentRetries[msg.Topic][msg.partition] != nil {
				if msg.flags&chaser == chaser {
					// we can start processing this topic/partition again
					currentRetries[msg.Topic][msg.partition] = nil
				}
				p.retryMessage(msg, currentRetries[msg.Topic][msg.partition])
				batch[i] = nil // to prevent it being returned/retried twice
				continue
			}

			partitionSet := msgSets[msg.Topic]
			if partitionSet == nil {
				partitionSet = make(map[int32][]*MessageToSend)
				msgSets[msg.Topic] = partitionSet
			}

			partitionSet[msg.partition] = append(partitionSet[msg.partition], msg)
		}

		request := p.buildRequest(msgSets)
		if request == nil {
			continue
		}

		response, err := broker.Produce(p.client.id, request)

		switch err {
		case nil:
			break
		case EncodingError:
			p.returnMessages(batch, err)
			continue
		default:
			p.client.disconnectBroker(broker)
			closing = err
			p.retryMessages(batch, err)
			continue
		}

		if response == nil {
			// this only happens when RequiredAcks is NoResponse, so we have to assume success
			if p.config.AckSuccesses {
				p.returnMessages(batch, nil)
			}
			continue
		}

		// we iterate through the blocks in the request, not the response, so that we notice
		// if the response is missing a block completely
		for topic, partitionSet := range msgSets {
			for partition, msgs := range partitionSet {

				block := response.GetBlock(topic, partition)
				if block == nil {
					p.returnMessages(msgs, IncompleteResponse)
					continue
				}

				switch block.Err {
				case NoError:
					// All the messages for this topic-partition were delivered successfully!
					if p.config.AckSuccesses {
						for i := range msgs {
							msgs[i].offset = block.Offset + int64(i)
						}
						p.returnMessages(msgs, nil)
					}
				case UnknownTopicOrPartition, NotLeaderForPartition, LeaderNotAvailable:
					if currentRetries[topic] == nil {
						currentRetries[topic] = make(map[int32]error)
					}
					currentRetries[topic][partition] = block.Err
					p.retryMessages(msgs, block.Err)
				default:
					p.returnMessages(msgs, block.Err)
				}
			}
		}
	}
	p.input <- &MessageToSend{flags: unref}
}

///////////////////////////////////////////
///////////////////////////////////////////

// utility functions

func (p *Producer) assignPartition(msg *MessageToSend) error {
	partitions, err := p.client.Partitions(msg.Topic)
	if err != nil {
		return err
	}

	numPartitions := int32(len(partitions))

	if numPartitions == 0 {
		return LeaderNotAvailable
	}

	choice := p.config.Partitioner.Partition(msg.Key, numPartitions)

	if choice < 0 || choice >= numPartitions {
		return InvalidPartition
	}

	msg.partition = partitions[choice]

	return nil
}

func (p *Producer) buildRequest(batch map[string]map[int32][]*MessageToSend) *ProduceRequest {

	req := &ProduceRequest{RequiredAcks: p.config.RequiredAcks, Timeout: int32(p.config.Timeout / time.Millisecond)}
	empty := true

	for topic, partitionSet := range batch {
		for partition, msgSet := range partitionSet {
			setToSend := new(MessageSet)
			setSize := 0
			for _, msg := range msgSet {
				var keyBytes, valBytes []byte
				var err error
				if msg.Key != nil {
					if keyBytes, err = msg.Key.Encode(); err != nil {
						p.errors <- &ProduceError{Msg: msg, Err: err}
						continue
					}
				}
				if msg.Value != nil {
					if valBytes, err = msg.Value.Encode(); err != nil {
						p.errors <- &ProduceError{Msg: msg, Err: err}
						continue
					}
				}

				if p.config.Compression != CompressionNone && setSize+msg.byteSize() > p.config.MaxMessageBytes {
					// compression causes message-sets to be wrapped as single messages, which have tighter
					// size requirements, so we have to respect those limits
					valBytes, err := encode(setToSend)
					if err != nil {
						Logger.Println(err) // if this happens, it's basically our fault.
						panic(err)
					}
					req.AddMessage(topic, partition, &Message{Codec: p.config.Compression, Key: nil, Value: valBytes})
					setToSend = new(MessageSet)
					setSize = 0
				}
				setSize += msg.byteSize()

				setToSend.addMessage(&Message{Codec: CompressionNone, Key: keyBytes, Value: valBytes})
				empty = false
			}

			if p.config.Compression == CompressionNone {
				req.AddSet(topic, partition, setToSend)
			} else {
				valBytes, err := encode(setToSend)
				if err != nil {
					Logger.Println(err) // if this happens, it's basically our fault.
					panic(err)
				}
				req.AddMessage(topic, partition, &Message{Codec: p.config.Compression, Key: nil, Value: valBytes})
			}
		}
	}

	if empty {
		return nil
	} else {
		return req
	}
}

func (p *Producer) returnMessages(batch []*MessageToSend, err error) {
	for _, msg := range batch {
		if msg == nil {
			continue
		}
		p.errors <- &ProduceError{Msg: msg, Err: err}
	}
}

func (p *Producer) retryMessages(batch []*MessageToSend, err error) {
	for _, msg := range batch {
		if msg == nil {
			continue
		}
		p.retryMessage(msg, err)
	}
}

func (p *Producer) retryMessage(msg *MessageToSend, err error) {
	if msg.flags&retried == retried {
		p.errors <- &ProduceError{Msg: msg, Err: err}
	} else {
		msg.flags |= retried
		p.input <- msg
	}
}
