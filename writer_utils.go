package sarama

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"strconv"
	"strings"
	"time"
)

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
func NewLogger(prefix string, out io.Writer) StdLogger {
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
