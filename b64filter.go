package main

import (
	"bufio"
	"encoding/base64"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"strings"
)

func init() {
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "Usage: %s [flags] filter [args]\n", os.Args[0])
		flag.PrintDefaults()
		fmt.Fprintf(flag.CommandLine.Output(),
`Runs the given program as a filter on the input. Standard input and output are
expected to be base 64 encoded, one document or record per line. The input is
passed in decoded form through the filter program, and then re-encoded.

Note that the filter program must be line-buffered for this to work. Otherwise
this program will wait on the filter's output forever.
`)
	}
}

func readDocs(r io.ReadCloser) (ch chan string) {
	ch = make(chan string)
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
						ch <- string(b[:n])
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
				ch <- string(b[:n])
				line = make([]byte, 0, 1024)
			}
		}
	}()
	return ch
}

func writeDoc(doc string, w io.Writer) (err error) {
	bdoc := []byte(doc)
	elen := base64.StdEncoding.EncodedLen(len(bdoc))
	b := make([]byte, elen, elen+1)
	base64.StdEncoding.Encode(b, bdoc)
	b = append(b, '\n')
	_, err = w.Write(b)
	return
}

func readNLines(count int, r io.Reader) (ch chan string) {
	ch = make(chan string)

	go func() {
		buf := bufio.NewReader(r)
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
						ch <- string(line)
					}
					break
				} else {
					log.Fatalf("error reading line: %v", err)
				}
			}
			// if we have a complete line, send it
			if !pfx {
				ch <- string(line)
				line = make([]byte, 0, 1024)
			}
		}
		close(ch)
	}()

	return
}

func filterDoc(indoc string, cmdin io.Writer, cmdout io.Reader) (outdoc string, err error) {
	inlines := strings.Split(indoc, "\n")
	go func() {
		for _, line := range inlines {
			cmdin.Write([]byte(line))
			cmdin.Write([]byte("\n"))
		}
	}()

	outlines := make([]string, 0, len(inlines))
	ch := readNLines(len(inlines), cmdout)
	for line := range ch {
		outlines = append(outlines, line)
	}

	if len(inlines) != len(outlines) {
		err = errors.New(fmt.Sprintf("output linecount (%d) is not equal to input line count (%d)", len(outlines), len(inlines)))
	}

	outdoc = strings.Join(outlines, "\n")

	return
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

	err = cmd.Start()
	if err != nil {
		log.Fatalf("error starting command: %v", err)
	}

	indocs := readDocs(os.Stdin)
	i := 0
	for indoc := range indocs {
		outdoc, err := filterDoc(indoc, cmdin, cmdout)
		if err != nil {
			log.Fatalf("error writing to command: %v", err)
		}
		err = writeDoc(outdoc, os.Stdout)
		if err != nil {
			log.Fatalf("error writing output: %v", err)
		}
		i += 1
	}
	cmdin.Close()

	if err = cmd.Wait(); err != nil {
		log.Fatalf("error waiting for command: %v", err)
	}

	log.Printf("processed %v documents", i)
}

