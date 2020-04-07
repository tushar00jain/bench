package bench

import (
	"sync"
	"time"

	"github.com/codahale/hdrhistogram"
)

func newDummyConnectionBenchmark(r Requester) *connectionBenchmark {
	return &connectionBenchmark{
		requester:                   r,
		successHistogram:            hdrhistogram.New(1, maxRecordableLatencyNS, sigFigs),
		uncorrectedSuccessHistogram: hdrhistogram.New(1, maxRecordableLatencyNS, sigFigs),
		errorHistogram:              hdrhistogram.New(1, maxRecordableLatencyNS, sigFigs),
		uncorrectedErrorHistogram:   hdrhistogram.New(1, maxRecordableLatencyNS, sigFigs),
	}
}

func (b *Benchmark) RunCustom(batches [][][]byte) (*Summary, error) {
	resultsCount := len(batches) * len(b.benchmarks)
	results := make(chan *result, resultsCount)

	// Prepare connection benchmarks
	for i, benchmark := range b.benchmarks {
		if err := benchmark.setup(i); err != nil {
			return nil, err
		}
	}

	for _, batch := range batches {
		for i, benchmark := range b.benchmarks {
			var wg sync.WaitGroup
			start := make(chan struct{})
			wg.Add(1)

			go func(i int, benchmark *connectionBenchmark) {
				defer wg.Done()
				<-start
				var err error
				d := newDummyConnectionBenchmark(benchmark.requester)
				d.elapsed, err = d.runCustom(batch[i])
				results <- &result{summary: d.summarize(), err: err}
			}(i, benchmark)

			// Start benchmark
			close(start)

			// Wait for completion
			wg.Wait()
		}
	}

	// Teardown
	for _, benchmark := range b.benchmarks {
		if err := benchmark.teardown(); err != nil {
			return nil, err
		}
	}

	// Merge results
	result := <-results
	if result.err != nil {
		return nil, result.err
	}
	summary := result.summary
	for i := 1; i < resultsCount; i++ {
		result = <-results
		if result.err != nil {
			return nil, result.err
		}
		summary.merge(result.summary)
	}
	summary.Connections = b.connections

	return summary, nil
}

func (c *connectionBenchmark) runCustom(request []byte) (time.Duration, error) {
	var (
		start = time.Now()
	)

	before := time.Now()
	err := c.requester.Request()
	latency := time.Since(before).Nanoseconds()
	if err != nil {
		if err := c.errorHistogram.RecordValue(latency); err != nil {
			return 0, err
		}
		c.errorTotal++
	} else {
		if err := c.successHistogram.RecordValue(latency); err != nil {
			return 0, err
		}
		c.successTotal++
	}

	return time.Since(start), nil
}
