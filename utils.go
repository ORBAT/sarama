package sarama

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/bradfitz/iter"
)

// make []int32 sortable so we can sort partition numbers
type int32Slice []int32

func (slice int32Slice) Len() int {
	return len(slice)
}

func (slice int32Slice) Less(i, j int) bool {
	return slice[i] < slice[j]
}

func (slice int32Slice) Swap(i, j int) {
	slice[i], slice[j] = slice[j], slice[i]
}

func dupeAndSort(input []int32) []int32 {
	ret := make([]int32, 0, len(input))
	for _, val := range input {
		ret = append(ret, val)
	}

	sort.Sort(int32Slice(ret))
	return ret
}

func withRecover(fn func()) {
	defer func() {
		handler := PanicHandler
		if handler != nil {
			if err := recover(); err != nil {
				handler(err)
			}
		}
	}()

	fn()
}

func safeAsyncClose(c io.Closer) {
	tmp := c // local var prevents clobbering in goroutine
	go withRecover(func() {
		if err := tmp.Close(); err != nil {
			Logger.Println("Error closing", tmp, ":", err)
		}
	})
}

// Encoder is a simple interface for any type that can be encoded as an array of bytes
// in order to be sent as the key or value of a Kafka message. Length() is provided as an
// optimization, and must return the same as len() on the result of Encode().
type Encoder interface {
	Encode() ([]byte, error)
	Length() int
}

// make strings and byte slices encodable for convenience so they can be used as keys
// and/or values in kafka messages

// StringEncoder implements the Encoder interface for Go strings so that you can do things like
//	producer.SendMessage(nil, sarama.StringEncoder("hello world"))
type StringEncoder string

func (s StringEncoder) Encode() ([]byte, error) {
	return []byte(s), nil
}

func (s StringEncoder) Length() int {
	return len(s)
}

// ByteEncoder implements the Encoder interface for Go byte slices so that you can do things like
//	producer.SendMessage(nil, sarama.ByteEncoder([]byte{0x00}))
type ByteEncoder []byte

func (b ByteEncoder) Encode() ([]byte, error) {
	return b, nil
}

func (b ByteEncoder) Length() int {
	return len(b)
}

var rng = rand.New(rand.NewSource(time.Now().UnixNano()))

// MultiError wraps multiple errors into one.
type MultiError struct {
	errStr string
	Errors []error
}

func (me *MultiError) Error() string {
	if len(me.errStr) == 0 {
		errStrs := make([]string, 0, len(me.Errors))
		for _, e := range me.Errors {
			errStrs = append(errStrs, e.Error())
		}
		me.errStr = strings.Join(errStrs, ",")
	}
	return me.errStr
}

// TimestampRandom returns a string with the timestamp in milliseconds, a "-", and a random integer with a maximum of 2^24.
func TimestampRandom() string {
	return strconv.Itoa(int(time.Now().UnixNano()/int64(time.Millisecond))) + "-" + strconv.Itoa(rng.Intn(1<<24))
}

// LogOutput is the default writer for loggers created with NewLogger(). Defaults to ioutil.Discard.
var LogOutput io.Writer = ioutil.Discard

// NewLogger creates a new log.Logger using either "out" or, if it's nil, LogOutput for writing.
func NewLogger(prefix string, out io.Writer) *log.Logger {
	if out == nil {
		out = LogOutput
	}
	return log.New(out, fmt.Sprintf("[%s] ", prefix), log.Ldate|log.Lmicroseconds)
}

// LogTo makes all of sarama's loggers output to the given writer by replacing Logger and LogOutput. It returns a function that
// sets LogOutput and Logger to whatever values they had before the call to LogTo.
//
// As an example
//     defer LogTo(os.Stderr)()
// would start logging to stderr immediately and defer restoring the loggers.
func LogTo(w io.Writer) func() {
	oldLogger := Logger
	oldOutput := LogOutput
	LogOutput = w
	Logger = NewLogger("Sarama", LogOutput)
	return func() {
		LogOutput = oldOutput
		Logger = oldLogger
	}
}

// CountingSemaphore allows limiting concurrent access to a resource by blocking acquiring if the limit has been met.
type CountingSemaphore struct {
	limit int
	ch    chan struct{}
}

// Acquire acquires the semaphore, potentially blocking if the limit has been met.
func (cs *CountingSemaphore) Acquire() {
	<-cs.ch
}

// Release releases the semaphore. If there are more Release calls than Acquire, Release will panic.
func (cs *CountingSemaphore) Release() {
	select {
	case cs.ch <- struct{}{}:
	default:
		panic(errors.New("Mismatched CountingSemaphore release"))
	}
}

// Count returns how many times the semaphore is currently acquired
func (cs *CountingSemaphore) Count() int {
	return cs.limit - len(cs.ch)
}

// Remaining returns how many times the semaphore can still be acquired
func (cs *CountingSemaphore) Remaining() int {
	return len(cs.ch)
}

// NewCountingSemaphore creates a new CountingSemaphore. If limit is < 1, it will be set to 1.
func NewCountingSemaphore(limit int) *CountingSemaphore {
	if limit < 1 {
		limit = 1
	}
	ch := make(chan struct{}, limit)
	for _ = range iter.N(limit) {
		ch <- struct{}{}
	}
	return &CountingSemaphore{limit: limit, ch: ch}
}
