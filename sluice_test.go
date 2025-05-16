package sluice

import (
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// mockSuccessfulBatchFunc is a helper for tests that successfully processes items.
func mockSuccessfulBatchFunc(inputs []int, args ...interface{}) (map[string]string, error) {
	results := make(map[string]string)
	prefix := "id-" // Default prefix
	if len(args) > 0 {
		if p, ok := args[0].(string); ok {
			prefix = p
		}
	}

	// Simulate some processing time
	time.Sleep(10 * time.Millisecond)

	for _, input := range inputs {
		id := prefix + strconv.Itoa(input)
		results[id] = "processed-" + strconv.Itoa(input)
	}
	return results, nil
}

// mockErroringBatchFunc is a helper that always returns a batch-level error.
func mockErroringBatchFunc(inputs []int, args ...interface{}) (map[string]string, error) {
	time.Sleep(5 * time.Millisecond)
	return nil, fmt.Errorf("simulated batch processing error")
}

// mockPanickingBatchFunc is a helper that always panics.
func mockPanickingBatchFunc(inputs []int, args ...interface{}) (map[string]string, error) {
	time.Sleep(5 * time.Millisecond)
	panic("simulated panic in BatchFunc")
}

// mockMissingIDBatchFunc processes some items but omits results for others.
func mockMissingIDBatchFunc(inputs []int, args ...interface{}) (map[string]string, error) {
	results := make(map[string]string)
	time.Sleep(10 * time.Millisecond)
	// Process only even inputs to simulate missing IDs for odd inputs
	for _, input := range inputs {
		if input%2 == 0 {
			id := "id-" + strconv.Itoa(input)
			results[id] = "processed-" + strconv.Itoa(input)
		}
	}
	return results, nil
}

func TestBatcher_BasicFunctionality_BatchBySize(t *testing.T) {
	t.Parallel()
	config := BatchProcessorConfig[int, string]{
		Func: mockSuccessfulBatchFunc,
	}
	// Interval is still present, but much shorter.
	// Since maxBatchSize is small (3), batching should primarily occur due to size.
	// The interval (e.g., 1 second) acts as a fallback for the last partial batch.
	b := NewBatcher(config, 1*time.Second, 3, 2) // Reduced interval from 1 minute to 1 second
	defer b.Stop()

	var wg sync.WaitGroup
	numItems := 7 // Expect 2 batches of 3, and 1 batch of 1.

	results := make(map[int]string)
	errs := make(map[int]error)
	var mu sync.Mutex // Protect map writes

	for i := 0; i < numItems; i++ {
		wg.Add(1)
		go func(val int) {
			defer wg.Done()
			id := "id-" + strconv.Itoa(val)
			res, err := b.SubmitAndAwait(id, val)

			mu.Lock()
			results[val] = res
			errs[val] = err
			mu.Unlock()
		}(i)
	}
	wg.Wait()

	for i := 0; i < numItems; i++ {
		if errs[i] != nil {
			t.Errorf("Item %d: expected no error, got %v", i, errs[i])
		}
		expected := "processed-" + strconv.Itoa(i)
		if results[i] != expected {
			t.Errorf("Item %d: expected result '%s', got '%s'", i, expected, results[i])
		}
	}
}

func TestBatcher_BatchByTime(t *testing.T) {
	t.Parallel()
	var batchProcessCount int32
	timedBatchFunc := func(inputs []int, args ...interface{}) (map[string]string, error) {
		atomic.AddInt32(&batchProcessCount, 1)
		return mockSuccessfulBatchFunc(inputs, args...)
	}
	config := BatchProcessorConfig[int, string]{
		Func: timedBatchFunc,
	}
	// Short interval, large batch size, so batching by time.
	b := NewBatcher(config, 50*time.Millisecond, 100, 1)
	defer b.Stop()

	var wg sync.WaitGroup
	numItems := 3
	for i := 0; i < numItems; i++ {
		wg.Add(1)
		go func(val int) {
			defer wg.Done()
			id := "id-" + strconv.Itoa(val)
			b.SubmitAndAwait(id, val) // We only care that it gets processed.
		}(i)
		time.Sleep(10 * time.Millisecond) // Stagger submissions.
	}
	wg.Wait()

	// Allow time for the ticker to trigger potentially multiple batches.
	time.Sleep(150 * time.Millisecond)

	processedCount := atomic.LoadInt32(&batchProcessCount)
	if processedCount == 0 {
		t.Errorf("Expected at least one batch to be processed by time, but count is 0")
	}
	t.Logf("Number of time-triggered batches processed: %d", processedCount)
}

func TestBatcher_BatchLevelError(t *testing.T) {
	t.Parallel()
	config := BatchProcessorConfig[int, string]{
		Func: mockErroringBatchFunc,
	}
	b := NewBatcher(config, 1*time.Second, 3, 1)
	defer b.Stop()

	id := "id-1"
	val, err := b.SubmitAndAwait(id, 1)

	if err == nil {
		t.Fatal("Expected an error from SubmitAndAwait due to batch-level error, got nil")
	}
	expectedErrMsg := "simulated batch processing error"
	if err.Error() != expectedErrMsg {
		t.Errorf("Expected error message '%s', got '%s'", expectedErrMsg, err.Error())
	}
	if val != "" { // Zero value for string
		t.Errorf("Expected zero value for output on batch error, got '%s'", val)
	}
}

func TestBatcher_PanicInBatchFunc(t *testing.T) {
	t.Parallel()
	config := BatchProcessorConfig[int, string]{
		Func: mockPanickingBatchFunc,
	}
	b := NewBatcher(config, 1*time.Second, 1, 1)
	defer b.Stop()

	_, err := b.SubmitAndAwait("id-panic", 1)
	if err == nil {
		t.Fatal("Expected an error from SubmitAndAwait due to panic, got nil")
	}
	expectedErrorMsg := "sluice.Batcher: panic during batch processing: simulated panic in BatchFunc"
	if err.Error() != expectedErrorMsg {
		t.Errorf("Expected error message '%s', got '%s'", expectedErrorMsg, err.Error())
	}
}

func TestBatcher_MissingIDInResult(t *testing.T) {
	t.Parallel()
	config := BatchProcessorConfig[int, string]{
		Func: mockMissingIDBatchFunc,
	}
	// Batch size 2 to ensure both items are in the same batch.
	b := NewBatcher(config, 1*time.Second, 2, 1)
	defer b.Stop()

	var wg sync.WaitGroup
	wg.Add(2)

	var errEven, errOdd error
	var resEven, resOdd string

	// Item 0 (even, should be processed)
	go func() {
		defer wg.Done()
		resEven, errEven = b.SubmitAndAwait("id-0", 0)
	}()
	// Item 1 (odd, should be missing from results)
	go func() {
		defer wg.Done()
		resOdd, errOdd = b.SubmitAndAwait("id-1", 1)
	}()
	wg.Wait()

	// Check item 0 (even)
	if errEven != nil {
		t.Errorf("Item 0 (even): expected no error, got %v", errEven)
	}
	expectedEvenRes := "processed-0"
	if resEven != expectedEvenRes {
		t.Errorf("Item 0 (even): expected result '%s', got '%s'", expectedEvenRes, resEven)
	}

	// Check item 1 (odd)
	if errOdd == nil {
		t.Errorf("Item 1 (odd): expected an error due to missing output, got nil")
	} else {
		expectedOddErrMsg := "sluice.Batcher: no output found for ID id-1"
		if errOdd.Error() != expectedOddErrMsg {
			t.Errorf("Item 1 (odd): expected error msg '%s', got '%s'", expectedOddErrMsg, errOdd.Error())
		}
	}
	if resOdd != "" { // Zero value for string
		t.Errorf("Item 1 (odd): expected zero value for output on missing ID, got '%s'", resOdd)
	}
}

func TestBatcher_StopBehavior_ProcessesRemainingItems(t *testing.T) {
	t.Parallel()
	config := BatchProcessorConfig[int, string]{
		Func: mockSuccessfulBatchFunc,
	}
	// Short interval, small batch size to make it likely items are pending when Stop is called.
	b := NewBatcher(config, 50*time.Millisecond, 2, 1)

	var wg sync.WaitGroup
	numItems := 5
	results := make(chan string, numItems)

	for i := 0; i < numItems; i++ {
		wg.Add(1)
		go func(val int) {
			defer wg.Done()
			id := "id-" + strconv.Itoa(val)
			res, err := b.SubmitAndAwait(id, val)
			if err != nil {
				// If Stop() is called very quickly, some later submissions might get "batcher stopped" error.
				// This test focuses on items submitted *before* Stop() fully takes effect.
				if err.Error() != "sluice.Batcher: Batcher has been stopped, not accepting new items" {
					t.Errorf("Item %d: unexpected error: %v", val, err)
				} else {
					t.Logf("Item %d: got expected 'stopped' error: %v", val, err)
				}
				results <- "error" // Indicate error or stopped
				return
			}
			results <- res
		}(i)
		if i < numItems-1 { // Don't sleep after last item, call Stop quickly
			time.Sleep(5 * time.Millisecond)
		}
	}

	time.Sleep(10 * time.Millisecond) // Allow some items to be enqueued
	b.Stop()                          // Stop the batcher

	wg.Wait()      // Wait for all SubmitAndAwait calls to return
	close(results) // Close results channel

	processedCount := 0
	for res := range results {
		if res != "error" { // Count only successfully processed items
			processedCount++
		}
	}

	// This assertion is tricky because the exact number processed before Stop fully kicks in can vary.
	// We expect *most* if not all to be processed. If 0, something is very wrong.
	if processedCount == 0 && numItems > 0 {
		t.Errorf("Expected some items to be processed before/during stop, but none were.")
	}
	t.Logf("%d out of %d items were processed successfully during/before stop.", processedCount, numItems)
}

func TestBatcher_StopBehavior_RejectsNewItems(t *testing.T) {
	t.Parallel()
	config := BatchProcessorConfig[int, string]{
		Func: mockSuccessfulBatchFunc,
	}
	b := NewBatcher(config, 1*time.Second, 5, 1)
	b.Stop() // Stop immediately

	_, err := b.SubmitAndAwait("id-after-stop", 1)
	if err == nil {
		t.Fatal("Expected an error when submitting to a stopped Batcher, got nil")
	}
	expectedErrMsg := "sluice.Batcher: Batcher has been stopped, not accepting new items"
	if err.Error() != expectedErrMsg {
		t.Errorf("Expected error message '%s', got '%s'", expectedErrMsg, err.Error())
	}
}

func TestBatcher_MaxWorkers_ConcurrencyLimit(t *testing.T) {
	t.Parallel()
	var currentConcurrent, maxObservedConcurrent int32
	var mu sync.Mutex // Protects maxObservedConcurrent writes

	// BatchFunc that tracks concurrency.
	concurrentTrackingFunc := func(inputs []int, args ...interface{}) (map[string]string, error) {
		current := atomic.AddInt32(&currentConcurrent, 1)
		mu.Lock()
		if current > maxObservedConcurrent {
			maxObservedConcurrent = current
		}
		mu.Unlock()

		time.Sleep(100 * time.Millisecond) // Hold the worker slot for a bit.
		atomic.AddInt32(&currentConcurrent, -1)
		return mockSuccessfulBatchFunc(inputs, args...) // Delegate actual processing
	}

	config := BatchProcessorConfig[int, string]{
		Func: concurrentTrackingFunc,
	}
	numWorkers := 3
	// Batch size 1 to force each item into its own batch, maximizing worker usage.
	// Short interval to quickly dispatch.
	b := NewBatcher(config, 10*time.Millisecond, 1, numWorkers)
	defer b.Stop()

	var wg sync.WaitGroup
	numItemsToStress := numWorkers * 5 // Submit many more items than workers.

	for i := 0; i < numItemsToStress; i++ {
		wg.Add(1)
		go func(val int) {
			defer wg.Done()
			id := "id-" + strconv.Itoa(val)
			b.SubmitAndAwait(id, val)
		}(i)
	}
	wg.Wait()

	// Allow any final batches to complete processing
	time.Sleep(200 * time.Millisecond)

	mu.Lock() // Read final maxObservedConcurrent safely
	observed := maxObservedConcurrent
	mu.Unlock()

	if observed > int32(numWorkers) {
		t.Errorf("Observed %d concurrent workers, but expected max %d", observed, numWorkers)
	}
	if observed == 0 && numItemsToStress > 0 && numWorkers > 0 {
		t.Logf("Warning: Max observed concurrent workers was 0. Test might not have effectively stressed concurrency.")
	}
	t.Logf("Max observed concurrent workers: %d (configured: %d)", observed, numWorkers)
}

func TestBatcher_Defaults_MinBatchSizeAndWorkers(t *testing.T) {
	t.Parallel()
	config := BatchProcessorConfig[int, string]{Func: mockSuccessfulBatchFunc}

	// Test with 0 maxBatchSize
	b1 := NewBatcher(config, 1*time.Second, 0, 1)
	if b1.maxBatchSize != 1 {
		t.Errorf("Expected maxBatchSize to default to 1 when 0, got %d", b1.maxBatchSize)
	}
	b1.Stop()

	// Test with negative maxBatchSize
	b2 := NewBatcher(config, 1*time.Second, -5, 1)
	if b2.maxBatchSize != 1 {
		t.Errorf("Expected maxBatchSize to default to 1 when negative, got %d", b2.maxBatchSize)
	}
	b2.Stop()

	// Test with 0 numWorkers
	b3 := NewBatcher(config, 1*time.Second, 1, 0)
	if cap(b3.workerPool) != 1 {
		t.Errorf("Expected numWorkers (cap(workerPool)) to default to 1 when 0, got %d", cap(b3.workerPool))
	}
	b3.Stop()

	// Test with negative numWorkers
	b4 := NewBatcher(config, 1*time.Second, 1, -5)
	if cap(b4.workerPool) != 1 {
		t.Errorf("Expected numWorkers (cap(workerPool)) to default to 1 when negative, got %d", cap(b4.workerPool))
	}
	b4.Stop()
}

func TestBatcher_SubmitToStoppedBatcher_AfterSomeOperations(t *testing.T) {
	t.Parallel()
	config := BatchProcessorConfig[int, string]{
		Func: mockSuccessfulBatchFunc,
	}
	b := NewBatcher(config, 50*time.Millisecond, 2, 1)

	// Submit a few items
	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(val int) {
			defer wg.Done()
			b.SubmitAndAwait("id-"+strconv.Itoa(val), val)
		}(i)
	}
	wg.Wait() // Wait for these initial items to be processed

	b.Stop() // Now stop the batcher

	// Try submitting again
	_, err := b.SubmitAndAwait("id-after-stop-and-ops", 100)
	if err == nil {
		t.Fatal("Expected error when submitting to a stopped Batcher after operations, got nil")
	}
	expectedErrMsg := "sluice.Batcher: Batcher has been stopped, not accepting new items"
	if err.Error() != expectedErrMsg {
		t.Errorf("Expected error message '%s', got '%s'", expectedErrMsg, err.Error())
	}
}
