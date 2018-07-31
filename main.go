package main

import (
	"crypto/sha256"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sync"
)

type Result struct {
	file   string
	sha256 string
}

func search(dir string, input chan<- string) {
	filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
		} else if info.Mode().IsRegular() {
			input <- path
		}
		return nil
	})
	close(input)
}

func startWorker(input <-chan string, results chan<- *Result, wg *sync.WaitGroup) {
	h := sha256.New()
	for file := range input {
		f, err := os.Open(file)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			continue
		}
		if _, err := io.Copy(h, f); err != nil {
			fmt.Fprintln(os.Stderr, err)
			f.Close()
			continue
		}
		f.Close()
		results <- &Result{
			file:   file,
			sha256: fmt.Sprintf("%x", h.Sum(nil)),
		}
		h.Reset()
	}
	wg.Done()
}

func run(dir string, workers int) (map[string][]string, error) {

	input := make(chan string)
	go search(dir, input)

	counter := make(map[string][]string)
	results := make(chan *Result)
	go func() {
		for r := range results {
			counter[r.sha256] = append(counter[r.sha256], r.file)
		}
	}()

	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go startWorker(input, results, &wg)
	}
	wg.Wait()
	close(results)

	return counter, nil
}

func main() {

	dir := flag.String("dir", ".", "directory to search")
	workers := flag.Int("workers", runtime.NumCPU(), "number of workers")
	flag.Parse()

	if *workers <= 0 {
		fmt.Printf("workers has to be > 0, was %d \n", workers)
		os.Exit(1)
	}
	fmt.Printf("Searching in %s using %d workers...\n", *dir, *workers)

	counter, err := run(*dir, *workers)
	if err != nil {
		fmt.Printf("failed! %v\n", err)
		os.Exit(1)
	}

	for sha, files := range counter {
		if len(files) > 1 {
			fmt.Printf("Found %d duplicates for %v: \n", len(files), sha)
			for _, f := range files {
				fmt.Println("-> ", f)
			}
		}
	}
}
