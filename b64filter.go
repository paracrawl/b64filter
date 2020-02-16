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
)

func init() {
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
				if err == io.EOF {
					if len(line) > 0 {
						b := make([]byte, base64.StdEncoding.DecodedLen(len(line)))
						n, err := base64.StdEncoding.Decode(b, line)
						if err != nil {
							log.Fatalf("error decoding line: %v", err)
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
					log.Fatalf("error decoding line: %v", err)
				}
				ch <- b[:n]
				line = make([]byte, 0, 1024)
			}
		}
	}()
	return ch
}

func readNLines(count int, buf *bufio.Reader) (lines [][]byte, err error) {
	lines = make([][]byte, 0, count)

	line := make([]byte, 0, 1024)
	for n := 0; n < count; n++ {
		chunk, pfx, err := buf.ReadLine()
		// we got some bytes, accumulate
		if len(chunk) > 0 {
			line = append(line, chunk...)
		}
		// we're done
		if err != nil {
			if err == io.EOF {
				if len(line) > 0 {
					lines = append(lines, line)
				}
				break
			} else {
				return nil, err
			}
		}
		// if we have a complete line, send it
		if !pfx {
			lines = append(lines, line)
			line = make([]byte, 0, 1024)
		}
	}

	return
}

func writeDocs(counts chan int, buf *bufio.Reader, w io.Writer) {
	for n := range counts {
		lines, err := readNLines(n, buf)
		if err != nil {
			log.Fatalf("error reading %v lines: %v", n, err)
		}
		if len(lines) != n {
			log.Fatalf("expected %v lines got %v", n, len(lines))
		}
		doc := bytes.Join(lines, []byte("\n"))

		elen := base64.StdEncoding.EncodedLen(len(doc))
		b := make([]byte, elen, elen+1)
		base64.StdEncoding.Encode(b, doc)
		b = append(b, '\n')
		_, err = w.Write(b)
		if err != nil {
			log.Fatalf("error writing %v lines %v", n, err)
		}
	}
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
			log.Fatalf("error processing standard error: %v", err)
		}
	}()
	
	err = cmd.Start()
	if err != nil {
		log.Fatalf("error starting command: %v", err)
	}

	counts := make(chan int, 256)
	buf := bufio.NewReader(cmdout)
	go writeDocs(counts, buf, os.Stdout)

	docs := readDocs(os.Stdin)
	i := 0
	for doc := range docs {
		lines := bytes.Count(doc, []byte("\n"))

		if _, err := cmdin.Write(doc); err != nil {
			log.Fatalf("error writing to filter: %v", err)
		}
		if _, err := cmdin.Write([]byte("\n")); err != nil {
			log.Fatalf("error writing to filter: %v", err)
		}

		counts <- lines + 1 // extra newline at end
		i += 1
	}
	close(counts)
	cmdin.Close()

	if err = cmd.Wait(); err != nil {
		log.Fatalf("error waiting for command: %v", err)
	}

	log.Printf("processed %v documents", i)
}

