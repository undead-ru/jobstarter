package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"sync"
	"time"
)

type counter struct {
	i int
	sync.Mutex
}

func main() {

	uri := "s3://"

	fn := flag.String("f", "", "Input File Name")
	flag.Parse()
	fileName := *fn
	if fileName == "" {
		fmt.Println("Usage: jobstarter -f <filename>")
		os.Exit(1)
	}

	inputFile, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("ERROR: Can't open input file: %v", err)
	}
	defer inputFile.Close()
	log.SetOutput(inputFile)

	s := bufio.NewScanner(inputFile)
	s.Split(bufio.ScanLines)

	done := counter{}
	total := 0
	withErr := counter{}

	for s.Scan() {

		go func(n int, inputString string) {

			output, err := exec.Command("aws", "s3", "ls", uri+inputString).CombinedOutput()

			if err != nil {
				fmt.Printf("\nCMD ERR [%04d] input: %v\nCMD ERR [%04d] output: %v\n", n, inputString, n, string(output))
				withErr.Lock()
				withErr.i++
				withErr.Unlock()
				return
			}

			outFileName := fmt.Sprintf("logs/%s_%04d.log", inputString, n)
			outputFile, err := os.OpenFile(outFileName, os.O_CREATE|os.O_WRONLY, 0666)
			if err != nil {
				fmt.Printf("\nERROR: Can't open file: %v\n", err)
				os.Exit(2)
			}
			defer outputFile.Close()
			buffer := new(bytes.Buffer)
			err = binary.Write(buffer, binary.LittleEndian, &output)
			if err != nil {
				fmt.Printf("\nERROR: Can't write buffer: %v\n", err)
				os.Exit(2)
			}
			_, err = outputFile.Write(buffer.Bytes())
			if err != nil {
				fmt.Printf("\nERROR: Can't write outputfile: %v\n", err)
				os.Exit(2)
			}

			done.Lock()
			done.i++
			done.Unlock()

		}(total, s.Text())

		total++

	}

	fmt.Printf("TOTAL JOBS started: %d\n", total)

	for {
		if done.i+withErr.i >= total {
			fmt.Println("\n==================================================\nJobs done:", done.i, "With error:", withErr.i, "Total jobs:", total)
			os.Exit(0)
		}
		time.Sleep(1 * time.Second)
	}

}
