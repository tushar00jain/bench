package main

import (
	"fmt"
	"time"

	"github.com/tushar00jain/bench"
	"github.com/tushar00jain/bench/requester"
)

func main() {
	// r := &requester.NATSRequesterFactory{
	// 	URL:         "nats://localhost:4222",
	// 	PayloadSize: 500,
	// 	Subject:     "benchmark",
	// }
	r := &requester.NOOPRequesterFactory{}

	benchmark := bench.NewBenchmark(r, 100, 1, 10*time.Second, 0)
	summary, err := benchmark.Run()
	if err != nil {
		panic(err)
	}

	fmt.Println(summary)
	summary.GenerateLatencyDistribution(nil, "noop.txt")
}
