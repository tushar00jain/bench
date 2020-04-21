package bench

import (
	"errors"
	"strconv"
	"sync"
	"time"

	"github.com/codahale/hdrhistogram"
)

type BatchCount struct {
	Error   int64
	Success int64
	NotSent int64
}

type NotInBatchErr struct {
	Msg string
}

func (e *NotInBatchErr) Error() string {
	return e.Msg
}

var notInBatchErr *NotInBatchErr

func newDummyConnectionBenchmark(r Requester) *connectionBenchmark {
	return &connectionBenchmark{
		requester:                   r,
		successHistogram:            hdrhistogram.New(1, maxRecordableLatencyNS, sigFigs),
		uncorrectedSuccessHistogram: hdrhistogram.New(1, maxRecordableLatencyNS, sigFigs),
		errorHistogram:              hdrhistogram.New(1, maxRecordableLatencyNS, sigFigs),
		uncorrectedErrorHistogram:   hdrhistogram.New(1, maxRecordableLatencyNS, sigFigs),
	}
}

func (b *Benchmark) RunCustom(batches [][][]byte) (*Summary, [][]string, error) {
	resultsCount := len(batches) * len(b.benchmarks)
	results := make(chan *result, resultsCount)

	// Prepare connection benchmarks
	for i, benchmark := range b.benchmarks {
		if err := benchmark.setup(i); err != nil {
			return nil, nil, err
		}
	}

	batchCounts := make([]BatchCount, len(batches))
	for i, batch := range batches {
		batchCount := BatchCount{}
		for i, benchmark := range b.benchmarks {
			var wg sync.WaitGroup
			start := make(chan struct{})
			wg.Add(1)

			go func(i int, benchmark *connectionBenchmark, batbatchCount *BatchCount) {
				defer wg.Done()
				<-start
				var err error
				d := newDummyConnectionBenchmark(benchmark.requester)
				d.elapsed, err = d.runCustom(batch[i], &batchCount)
				results <- &result{summary: d.summarize(), err: err}
			}(i, benchmark, &batchCount)

			// Start benchmark
			close(start)

			// Wait for completion
			wg.Wait()
		}
		batchCounts[i] = batchCount
	}

	// Teardown
	for _, benchmark := range b.benchmarks {
		if err := benchmark.teardown(); err != nil {
			return nil, nil, err
		}
	}

	sbc := CreateBarChart(batchCounts)

	// Merge results
	var summary *Summary
	for i := 0; i < resultsCount; i++ {
		result := <-results
		if result.err != nil {
			if ok := errors.As(result.err, &notInBatchErr); ok {
				continue
			}
			return nil, nil, result.err
		}

		if summary == nil {
			summary = result.summary
		} else {
			summary.merge(result.summary)
		}
	}
	summary.Connections = b.connections

	return summary, sbc, nil
}

func CreateBarChart(batchCounts []BatchCount) [][]string {
	result := make([][]string, len(batchCounts)+1)
	result[0] = []string{"Success", "Error", "Not Sent"}

	for i, batchCount := range batchCounts {
		result[i+1] = []string{
			strconv.FormatInt(batchCount.Success, 10),
			strconv.FormatInt(batchCount.Error, 10),
			strconv.FormatInt(batchCount.NotSent, 10),
		}
	}

	return result
}

func (c *connectionBenchmark) runCustom(request []byte, batchCount *BatchCount) (time.Duration, error) {
	start := time.Now()
	before := time.Now()

	err := c.requester.Request(request)
	latency := time.Since(before).Nanoseconds()

	if err != nil {
		if ok := errors.As(err, &notInBatchErr); ok {
			batchCount.NotSent++
			return 0, err
		}

		if err := c.errorHistogram.RecordValue(latency); err != nil {
			return 0, err
		}
		batchCount.Error++
		c.errorTotal++
	} else {
		if err := c.successHistogram.RecordValue(latency); err != nil {
			return 0, err
		}
		batchCount.Success++
		c.successTotal++
	}

	return time.Since(start), nil
}
