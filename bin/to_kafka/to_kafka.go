package main

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"github.com/ORBAT/sarama"

	f "github.com/jessevdk/go-flags"
)

type Char byte

func (c *Char) MarshalFlag() (s string, err error) {
	s = fmt.Sprintf("%+q", byte(*c))[1:]
	s = s[:len(s)-1]
	return
}

func (c *Char) UnmarshalFlag(value string) (err error) {
	if len(value) > 1 && value[0] != '\\' {
		return fmt.Errorf("Delimiter (%s) must be either 1 character or an escape sequence like \\n or \\x00. Length was %d, first char %c", value, len(value), value[0])
	}

	uq, err := strconv.Unquote(`"` + value + `"`)
	if err != nil {
		return
	}

	if len(uq) != 1 {
		return fmt.Errorf("Delimiter ('%s') produced a multibyte result", value)
	}

	*c = Char(uq[0])

	return
}

var opts struct {
	Brokers   []string `short:"b" long:"brokers" description:"Broker addresses (can be given multiple times)" default:"localhost:9092"`
	ClientId  string   `short:"c" long:"clientid" description:"Client ID to use" default:"to_kafka_client"`
	Delimiter Char     `short:"d" long:"delimiter" default:"\\n" description:"Delimiter byte to use when reading stdin."`
	Verbose   bool     `short:"v" long:"verbose" default:"false" description:"Print extra debugging cruft to stderr"`
	Args      struct {
		Topic string `name:"topic" description:"The topic to write to" required:"true"`
	} `positional-args:"true"`
}

func main() {
	if _, err := f.Parse(&opts); err != nil {
		os.Exit(1)
	}

	if opts.Verbose {
		log.SetFlags(log.Ldate | log.Lmicroseconds)
		log.SetOutput(os.Stderr)
		sarama.LogTo(os.Stderr)
	} else {
		log.SetOutput(ioutil.Discard)
		sarama.LogTo(ioutil.Discard)
	}

	dels, _ := opts.Delimiter.MarshalFlag()
	log.Printf("Delimiter: '%s' (%#x), brokers: %s\ntopic: %s\nclient ID: %s\n", dels, byte(opts.Delimiter), strings.Join(opts.Brokers, ", "), opts.Args.Topic,
		opts.ClientId)

	cl, err := sarama.NewClient(opts.ClientId, opts.Brokers, nil)
	if err != nil {
		fmt.Printf("Error creating client: %s", err.Error())
		os.Exit(2)
	}

	writer, err := cl.NewQueuingWriter(opts.Args.Topic, nil)
	if err != nil {
		fmt.Printf("Error creating producer: %s", err.Error())
		os.Exit(2)
	}

	termCh := make(chan os.Signal, 1)
	signal.Notify(termCh, syscall.SIGINT, syscall.SIGTERM)

	stopCh := make(chan struct{})

	r := bufio.NewReader(os.Stdin)

	defer func() {
		if err := writer.CloseAll(); err != nil {
			panic(err)
		}
	}()

	go func() {
		totalN := int64(0)
		for {
			n, err := readAndPub(r, byte(opts.Delimiter), writer)
			totalN += int64(n)

			if err != nil {
				if err != io.EOF {
					fmt.Printf("error after %d bytes: %s\n", totalN, err.Error())
				}
				log.Printf("Wrote %d bytes to Kafka", totalN)
				close(stopCh)
				return
			}
		}
	}()

	select {
	case sig := <-termCh:
		log.Println(sig.String())
	case <-stopCh:
		log.Println("Stopping")
	}
}

func readAndPub(r *bufio.Reader, delim byte, p io.Writer) (n int, err error) {

	line, err := r.ReadBytes(delim)
	if len(line) > 0 && line[len(line)-1] == delim {
		line = line[:len(line)-1] // remove delimiter
	}
	if len(line) == 0 {
		return
	}
	if err != nil && err != io.EOF {
		return
	}
	n, err = p.Write(line)
	return
}
