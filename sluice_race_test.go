package sluice

import (
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestBatcher_RaceConditions tests for various race conditions we've identified and fixed.
// These tests serve as regression tests to ensure that future changes don't reintroduce
// the race conditions that were previously present in the codebase.
//
// Test Coverage:
//  1. BatchSizeOverflow_SelectOrdering: Verifies that batch sizes never exceed maxBatchSize
//     even when Go's pseudo-random select ordering might choose itemChannel over triggerChannel
//  2. ConcurrentMapAccess_TimerCallbacks: Tests for race conditions between timer callbacks
//     and main operations accessing the same maps
//  3. ChannelBlocking_HighConcurrency: Ensures trigger channels don't block under high load
//     with many concurrent shards
//  4. StopCoordination_Race: Tests for race conditions during shutdown coordination
//  5. TriggerPriority_Deterministic: Verifies that triggers are always processed before
//     new items to maintain batch size guarantees
func TestBatcher_RaceConditions(t *testing.T) {
	t.Run("BatchSizeOverflow_SelectOrdering", testBatchSizeOverflowSelectOrdering)
	t.Run("ConcurrentMapAccess_TimerCallbacks", testConcurrentMapAccessTimerCallbacks)
	t.Run("ChannelBlocking_HighConcurrency", testChannelBlockingHighConcurrency)
	t.Run("StopCoordination_Race", testStopCoordinationRace)
	t.Run("TriggerPriority_Deterministic", testTriggerPriorityDeterministic)
}

// testBatchSizeOverflowSelectOrdering tests that batch sizes never exceed maxBatchSize
// even under high concurrency where select ordering could cause issues
func testBatchSizeOverflowSelectOrdering(t *testing.T) {
	const maxBatchSize = 10
	const numItems = 100
	const numWorkers = 5

	var maxObservedBatchSize int64
	var totalBatches int64

	config := BatchProcessorConfig[int, string]{
		Func: func(inputs []int, args ...interface{}) (map[string]string, error) {
			// Track the maximum batch size we've seen
			batchSize := int64(len(inputs))
			for {
				current := atomic.LoadInt64(&maxObservedBatchSize)
				if batchSize <= current || atomic.CompareAndSwapInt64(&maxObservedBatchSize, current, batchSize) {
					break
				}
			}
			atomic.AddInt64(&totalBatches, 1)

			// Add some processing delay to increase chance of race conditions
			time.Sleep(5 * time.Millisecond)

			results := make(map[string]string)
			for _, input := range inputs {
				results[fmt.Sprintf("item-%d", input)] = fmt.Sprintf("processed-%d", input)
			}
			return results, nil
		},
		CommonArgs: []interface{}{"test"},
		KeyFunc: func(item int) string {
			// Use multiple shards to increase concurrency
			return fmt.Sprintf("shard-%d", item%3)
		},
	}

	batcher := NewBatcher(config, 50*time.Millisecond, maxBatchSize, numWorkers)
	defer batcher.Stop()

	// Send items concurrently from multiple goroutines to stress test select ordering
	var wg sync.WaitGroup
	for worker := 0; worker < numWorkers; worker++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for i := 0; i < numItems/numWorkers; i++ {
				itemID := workerID*100 + i
				_, err := batcher.SubmitAndAwait(fmt.Sprintf("item-%d", itemID), itemID)
				if err != nil {
					t.Errorf("Failed to submit item %d: %v", itemID, err)
					return
				}
			}
		}(worker)
	}

	wg.Wait()

	// Verify that no batch exceeded the maximum size
	observedMax := atomic.LoadInt64(&maxObservedBatchSize)
	if observedMax > int64(maxBatchSize) {
		t.Errorf("Batch size overflow detected: observed max batch size %d, expected max %d", observedMax, maxBatchSize)
	}

	totalProcessed := atomic.LoadInt64(&totalBatches)
	t.Logf("Processed %d batches with max observed batch size: %d (limit: %d)", totalProcessed, observedMax, maxBatchSize)
}

// testConcurrentMapAccessTimerCallbacks tests for race conditions between timer callbacks and main operations
func testConcurrentMapAccessTimerCallbacks(t *testing.T) {
	const maxBatchSize = 5
	const batchInterval = 20 * time.Millisecond
	const numItems = 50

	var raceDetected int64

	config := BatchProcessorConfig[int, string]{
		Func: func(inputs []int, args ...interface{}) (map[string]string, error) {
			// Simulate processing time
			time.Sleep(10 * time.Millisecond)

			results := make(map[string]string)
			for _, input := range inputs {
				results[fmt.Sprintf("timer-test-%d", input)] = fmt.Sprintf("processed-%d", input)
			}
			return results, nil
		},
		KeyFunc: func(item int) string {
			// Multiple shards to trigger more timers
			return fmt.Sprintf("timer-shard-%d", item%4)
		},
	}

	batcher := NewBatcher(config, batchInterval, maxBatchSize, 3)
	defer batcher.Stop()

	// Send items with irregular timing to trigger both size-based and timer-based batching
	var wg sync.WaitGroup
	for i := 0; i < numItems; i++ {
		wg.Add(1)
		go func(itemNum int) {
			defer wg.Done()

			// Add some random delay to create race conditions between timers and additions
			if itemNum%3 == 0 {
				time.Sleep(time.Duration(itemNum%10) * time.Millisecond)
			}

			_, err := batcher.SubmitAndAwait(fmt.Sprintf("timer-test-%d", itemNum), itemNum)
			if err != nil {
				atomic.AddInt64(&raceDetected, 1)
				t.Errorf("Race condition detected in timer processing for item %d: %v", itemNum, err)
			}
		}(i)
	}

	wg.Wait()

	if atomic.LoadInt64(&raceDetected) > 0 {
		t.Errorf("Race conditions detected in timer callback processing")
	}

	t.Log("Timer callback race condition test completed successfully")
}

// testChannelBlockingHighConcurrency tests that trigger channels don't block under high load
func testChannelBlockingHighConcurrency(t *testing.T) {
	const maxBatchSize = 3
	const numShards = 20 // More shards than trigger channel buffer
	const itemsPerShard = 10

	var blockedOperations int64
	var processedBatches int64

	config := BatchProcessorConfig[int, string]{
		Func: func(inputs []int, args ...interface{}) (map[string]string, error) {
			atomic.AddInt64(&processedBatches, 1)
			// Simulate variable processing time
			time.Sleep(time.Duration(len(inputs)) * time.Millisecond)

			results := make(map[string]string)
			for _, input := range inputs {
				results[fmt.Sprintf("blocking-test-%d", input)] = fmt.Sprintf("processed-%d", input)
			}
			return results, nil
		},
		KeyFunc: func(item int) string {
			return fmt.Sprintf("blocking-shard-%d", item%numShards)
		},
	}

	batcher := NewBatcher(config, 100*time.Millisecond, maxBatchSize, 2)
	defer batcher.Stop()

	// Send many items quickly to many shards to stress-test channel capacity
	var wg sync.WaitGroup
	startTime := time.Now()

	for shard := 0; shard < numShards; shard++ {
		for item := 0; item < itemsPerShard; item++ {
			wg.Add(1)
			go func(s, i int) {
				defer wg.Done()

				itemID := s*1000 + i
				timeout := time.NewTimer(5 * time.Second)
				defer timeout.Stop()

				done := make(chan error, 1)
				go func() {
					_, err := batcher.SubmitAndAwait(fmt.Sprintf("blocking-test-%d", itemID), itemID)
					done <- err
				}()

				select {
				case err := <-done:
					if err != nil {
						atomic.AddInt64(&blockedOperations, 1)
						t.Errorf("Operation blocked/failed for item %d: %v", itemID, err)
					}
				case <-timeout.C:
					atomic.AddInt64(&blockedOperations, 1)
					t.Errorf("Operation timed out for item %d - possible channel blocking", itemID)
				}
			}(shard, item)
		}
	}

	wg.Wait()

	duration := time.Since(startTime)
	blocked := atomic.LoadInt64(&blockedOperations)
	processed := atomic.LoadInt64(&processedBatches)

	if blocked > 0 {
		t.Errorf("Channel blocking detected: %d operations blocked/timed out", blocked)
	}

	t.Logf("High concurrency test completed in %v: processed %d batches, %d blocked operations",
		duration, processed, blocked)
}

// testStopCoordinationRace tests for race conditions during shutdown
func testStopCoordinationRace(t *testing.T) {
	const maxBatchSize = 5
	const numItems = 20

	var processedAfterStop int64
	var totalProcessed int64

	config := BatchProcessorConfig[int, string]{
		Func: func(inputs []int, args ...interface{}) (map[string]string, error) {
			atomic.AddInt64(&totalProcessed, int64(len(inputs)))
			// Simulate processing time
			time.Sleep(20 * time.Millisecond)

			results := make(map[string]string)
			for _, input := range inputs {
				results[fmt.Sprintf("stop-test-%d", input)] = fmt.Sprintf("processed-%d", input)
			}
			return results, nil
		},
		KeyFunc: func(item int) string {
			return fmt.Sprintf("stop-shard-%d", item%2)
		},
	}

	batcher := NewBatcher(config, 100*time.Millisecond, maxBatchSize, 3)

	// Start sending items
	var wg sync.WaitGroup
	stopTime := time.Now().Add(30 * time.Millisecond)

	// Send items concurrently
	for i := 0; i < numItems; i++ {
		wg.Add(1)
		go func(itemNum int) {
			defer wg.Done()

			result, err := batcher.SubmitAndAwait(fmt.Sprintf("stop-test-%d", itemNum), itemNum)

			// Check if we got a successful result after stop time
			if time.Now().After(stopTime) && err == nil && result != "" {
				atomic.AddInt64(&processedAfterStop, 1)
			}
		}(i)
	}

	// Stop the batcher while items are being processed
	time.Sleep(30 * time.Millisecond)
	batcher.Stop()

	wg.Wait()

	total := atomic.LoadInt64(&totalProcessed)
	afterStop := atomic.LoadInt64(&processedAfterStop)

	// Verify that some items were processed (proving the system worked)
	if total == 0 {
		t.Error("No items were processed - system may not be working")
	}

	t.Logf("Stop coordination test completed: %d total items processed, %d processed after stop time", total, afterStop)
}

// testTriggerPriorityDeterministic tests that triggers are always processed before new items
func testTriggerPriorityDeterministic(t *testing.T) {
	const maxBatchSize = 4
	const numRounds = 10

	var orderViolations int64
	var totalBatches int64
	var maxBatchSize_observed int64

	config := BatchProcessorConfig[int, string]{
		Func: func(inputs []int, args ...interface{}) (map[string]string, error) {
			batchSize := int64(len(inputs))
			atomic.AddInt64(&totalBatches, 1)

			// Track maximum batch size observed
			for {
				current := atomic.LoadInt64(&maxBatchSize_observed)
				if batchSize <= current || atomic.CompareAndSwapInt64(&maxBatchSize_observed, current, batchSize) {
					break
				}
			}

			// Check for priority ordering violations (batch size > maxBatchSize indicates
			// that triggers weren't processed before new items were added)
			if len(inputs) > maxBatchSize {
				atomic.AddInt64(&orderViolations, 1)
			}

			// Minimal processing time to allow race conditions to manifest
			time.Sleep(2 * time.Millisecond)

			results := make(map[string]string)
			for _, input := range inputs {
				results[fmt.Sprintf("priority-test-%d", input)] = fmt.Sprintf("processed-%d", input)
			}
			return results, nil
		},
		KeyFunc: func(item int) string {
			return fmt.Sprintf("priority-shard-%d", item%2)
		},
	}

	batcher := NewBatcher(config, 30*time.Millisecond, maxBatchSize, 2)
	defer batcher.Stop()

	// Rapidly send items to create contention between triggers and new items
	var wg sync.WaitGroup
	for round := 0; round < numRounds; round++ {
		wg.Add(1)
		go func(r int) {
			defer wg.Done()

			// Send exactly maxBatchSize items rapidly to trigger size-based batching
			for i := 0; i < maxBatchSize+2; i++ { // Send 2 extra to test overflow protection
				itemID := r*100 + i
				_, err := batcher.SubmitAndAwait(fmt.Sprintf("priority-test-%d", itemID), itemID)
				if err != nil {
					t.Errorf("Failed to process item %d: %v", itemID, err)
					return
				}
			}
		}(round)
	}

	wg.Wait()

	violations := atomic.LoadInt64(&orderViolations)
	batches := atomic.LoadInt64(&totalBatches)
	maxObserved := atomic.LoadInt64(&maxBatchSize_observed)

	if violations > 0 {
		t.Errorf("Trigger priority violations detected: %d batches exceeded max size", violations)
	}

	if maxObserved > int64(maxBatchSize) {
		t.Errorf("Batch size overflow: observed max %d, expected max %d", maxObserved, maxBatchSize)
	}

	t.Logf("Priority deterministic test completed: processed %d batches, max size observed: %d, violations: %d",
		batches, maxObserved, violations)
}

// TestBatcher_RaceConditions_WithRaceDetector runs the race condition tests specifically
// with the race detector to ensure they don't trigger false positives
func TestBatcher_RaceConditions_WithRaceDetector(t *testing.T) {
	// This test is designed to be run with: go test -race -run TestBatcher_RaceConditions_WithRaceDetector
	// It focuses on scenarios that previously triggered race detector warnings

	t.Run("ConcurrentMapAccess_Focused", func(t *testing.T) {
		// Focused test for map access patterns that previously caused races
		const maxBatchSize = 3
		const batchInterval = 10 * time.Millisecond

		config := BatchProcessorConfig[int, string]{
			Func: func(inputs []int, args ...interface{}) (map[string]string, error) {
				time.Sleep(5 * time.Millisecond)
				results := make(map[string]string)
				for _, input := range inputs {
					results[strconv.Itoa(input)] = fmt.Sprintf("race-test-%d", input)
				}
				return results, nil
			},
			KeyFunc: func(item int) string {
				return fmt.Sprintf("race-shard-%d", item%3)
			},
		}

		batcher := NewBatcher(config, batchInterval, maxBatchSize, 2)
		defer batcher.Stop()

		// Send items that will trigger both timer and size-based batching
		var wg sync.WaitGroup
		for i := 0; i < 20; i++ {
			wg.Add(1)
			go func(itemNum int) {
				defer wg.Done()
				_, err := batcher.SubmitAndAwait(strconv.Itoa(itemNum), itemNum)
				if err != nil {
					t.Errorf("Race detector test failed for item %d: %v", itemNum, err)
				}
			}(i)
		}

		wg.Wait()
		t.Log("Race detector focused test completed successfully")
	})
}
