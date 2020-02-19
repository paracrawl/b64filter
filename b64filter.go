package main

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"time"
	"github.com/golang-collections/go-datastructures/queue"
)

var debug bool
var progress int

func init() {
	flag.BoolVar(&debug, "d", false, "Debugging output")
	flag.IntVar(&progress, "p", 100, "Report progress every p files")
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "Usage: %s filter [args]\n", os.Args[0])
		flag.PrintDefaults()
		fmt.Fprintf(flag.CommandLine.Output(),
`
Runs the given program as a filter on the input. Standard input and output are
expected to be base 64 encoded, one document or record per line. The input is
passed in decoded form through the filter program, and then re-encoded.

Example:

    $ < test b64filter cat > test.cat
    2020/02/16 12:15:29 b64filter.go:188: processed 2 documents
    $ diff test test.cat
    $

The filter program is executed once and fed input from the entire set of
documents. This means that the filter must produce exactly one line of output
per line of input.
`)
	}
}

func readDocs(r io.ReadCloser) (ch chan []byte) {
	ch = make(chan []byte)
	go func() {
		buf := bufio.NewReader(r)

		line := make([]byte, 0, 1024)
		for {
			chunk, pfx, err := buf.ReadLine()
			// we got some bytes, accumulate
			if len(chunk) > 0 {
				line = append(line, chunk...)
			}
			// we're done
			if err != nil {
				if debug {
					log.Printf("readDocs: finished (%v)", err)
				}
				if err == io.EOF {
					if len(line) > 0 {
						b := make([]byte, base64.StdEncoding.DecodedLen(len(line)))
						n, err := base64.StdEncoding.Decode(b, line)
						if err != nil {
							log.Fatalf("readDocs: error decoding line (%v)", err)
						}
						ch <- b[:n]
					}
				} else {
					log.Fatalf("error reading line: %v", err)
				}
				close(ch)
				return
			}
			// if we have a complete line, send it
			if !pfx {
				b := make([]byte, base64.StdEncoding.DecodedLen(len(line)))
				n, err := base64.StdEncoding.Decode(b, line)
				if err != nil {
					log.Fatalf("readDocs: error decoding line (%v)", err)
				}
				ch <- b[:n]
				line = make([]byte, 0, 1024)
			}
		}
	}()
	return ch
}

func readNLines(count int, buf *bufio.Reader) (lines [][]byte, err error) {
	if debug {
		log.Printf("readNLines: reading %v lines", count)
	}

	lines = make([][]byte, 0, count)

	line := make([]byte, 0, 1024)
	for n := 0; n < count; n++ {
//		if debug {
//			log.Printf("readNLines: reading line %d", n)
//		}
		chunk, pfx, err := buf.ReadLine()
		// accumulate bytes
		line = append(line, chunk...)
		// we're done
		if err != nil {
			if err == io.EOF {
				lines = append(lines, line)
				break
			} else {
				return nil, err
			}
		}
		// if we have a complete line, send it
		if !pfx {
			lines = append(lines, line)
			line = make([]byte, 0, 1024)
		} else { // don't have a complete line, loop again
			n--
		}
	}

	if debug {
		log.Printf("readNLines: read %v lines", len(lines))
	}
	return
}

func writeDocs(counts *queue.Queue, done chan bool, buf *bufio.Reader, w io.Writer) {
	ndocs := 0
	nlines := 0
	start := time.Now()
	for {
		// get a line count off the queue
		ns, err := counts.Get(1)
		if err != nil {
			if counts.Disposed() {
				if debug {
					log.Printf("writeDocs: write queue finished")
				}
				break
			} else {
				log.Fatalf("writeDocs: error reading from queue: %v", err)
			}
		}
		if len(ns) != 1 {
			log.Fatalf("writeDocs: asked for 1 item, got %d", len(ns))
		}
		n := ns[0].(int)
		if debug {
			log.Printf("writeDocs: processing %d line document (qsize: %d)", n, counts.Len())
		}

		// read n lines of the document
		lines, err := readNLines(n, buf)
		if err != nil {
			log.Fatalf("writeDocs: error reading %v lines: %v", n, err)
		}
		if len(lines) != n {
			log.Fatalf("writeDocs: expected %v lines got %v", n, len(lines))
		}
		doc := bytes.Join(lines, []byte("\n"))

		// encode and output
		elen := base64.StdEncoding.EncodedLen(len(doc))
		b := make([]byte, elen, elen+1)
		base64.StdEncoding.Encode(b, doc)
		b = append(b, '\n')
		_, err = w.Write(b)
		if err != nil {
			log.Fatalf("writeDocs: error writing %v lines %v", n, err)
		}

		ndocs += 1
		nlines += n
		if progress > 0 && ndocs % progress == 0 {
			now := time.Now()
			log.Printf("writeDocs: written %d docs, %d lines in %s", ndocs, nlines, now.Sub(start).String())
		}
	}
	if debug {
		log.Printf("writeDocs: finished")
	}
	done <- true
}

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
	flag.Parse()

	if flag.NArg() < 1 {
		flag.Usage()
		os.Exit(-1)
	}

	args := flag.Args()
	cmd := exec.Command(args[0], args[1:]...)
	cmdin, err := cmd.StdinPipe()
	if err != nil {
		log.Fatalf("error getting command input: %v", err)
	}
	cmdout, err := cmd.StdoutPipe()
	if err != nil {
		log.Fatalf("error getting command output: %v", err)
	}
	cmderr, err := cmd.StderrPipe()
	if err != nil {
		log.Fatalf("error getting command standard error: %v", err)
	}

	// preserve stderr
	go func() {
		_, err := io.Copy(os.Stderr, cmderr)
		if err != nil {
			log.Printf("error processing standard error: %v", err)
		}
	}()

	err = cmd.Start()
	if err != nil {
		log.Fatalf("error starting command: %v", err)
	}

	counts := queue.New(32)
	done := make(chan bool)
	buf := bufio.NewReader(cmdout)
	go writeDocs(counts, done, buf, os.Stdout)

	docs := readDocs(os.Stdin)
	i := 0
	for doc := range docs {
		lines := bytes.Count(doc, []byte("\n"))
		if debug {
			log.Printf("main: writing %d line document to filter (qsize: %d)", lines+1, counts.Len())
		}

		if _, err := cmdin.Write(doc); err != nil {
			log.Fatalf("error writing to filter: %v", err)
		}
		if _, err := cmdin.Write([]byte("\n")); err != nil {
			log.Fatalf("error writing to filter: %v", err)
		}

		counts.Put(lines + 1) // extra newline at end
		i += 1
	}
	cmdin.Close()

	// wait for the queue to drain
	for {
		if counts.Empty() {
			counts.Dispose()
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	// it is required that all reading from the command is done before
	// calling Wait(). 
	_ = <-done
	if err = cmd.Wait(); err != nil {
		log.Fatalf("error waiting for command: %v", err)
	}

	log.Printf("processed %v documents", i)
}

