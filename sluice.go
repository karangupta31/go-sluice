package sluice

import (
	"fmt"
	"log"
	"time"
)

// BatchFunc defines the function signature for the batch processing function.
// It takes a slice of inputs of type T, any common arguments, and returns a map
// where keys are item IDs (strings) and values are the processed outputs of type Q.
// It also returns an error if the entire batch processing encountered a critical failure.
type BatchFunc[T any, Q any] func(inputs []T, commonArgs ...interface{}) (map[string]Q, error)

// BatchKeyFunc defines a function that returns a key for sharding/grouping items.
type BatchKeyFunc[T any] func(item T) string

// BatchProcessorConfig holds the user-defined BatchFunc and any common arguments
// that should be passed to every invocation of the BatchFunc.
type BatchProcessorConfig[T any, Q any] struct {
	// Func is the function that will be called to process a batch of items.
	Func BatchFunc[T, Q]
	// CommonArgs are arguments that will be passed to every call of the BatchFunc.
	// This is useful for passing dependencies like database connections or configurations.
	CommonArgs []interface{}
	// KeyFunc is an optional function to determine the batch/shard key for each item.
	// If nil, all items are batched together.
	KeyFunc BatchKeyFunc[T]

	// CleanupConfig holds optional configuration for batch key cleanup when sharding is enabled.
	// If nil, default cleanup parameters will be used.
	CleanupConfig *CleanupConfig
}

// CleanupConfig holds configuration parameters for batch key cleanup.
type CleanupConfig struct {
	// MaxKeysBeforeCleanup is the threshold for triggering LRU cleanup.
	// Default: 1000
	MaxKeysBeforeCleanup int
	// KeyInactiveThreshold is how long a key must be unused before it's eligible for cleanup.
	// Default: 10 minutes
	KeyInactiveThreshold time.Duration
	// CleanupInterval is how often to check for inactive keys.
	// Default: 5 minutes
	CleanupInterval time.Duration
}

// BatchItem represents a single input item submitted to the Batcher.
// It includes a unique ID for correlating input to output, the actual input data,
// and channels for delivering the processed output or any error encountered.
// This type is primarily used internally by the Batcher.
type BatchItem[T any, Q any] struct {
	// ID is a unique identifier for the item, used to map results back from the BatchFunc.
	ID string
	// Input is the raw data for this item.
	Input T
	// Output is a channel where the processed result (type Q) will be sent.
	Output chan Q
	// Error is a channel where any error specific to this item's processing (or a batch-level error) will be sent.
	Error chan error
}

// Batcher handles the collection of individual items, grouping them into batches,
// and processing these batches concurrently using a pool of worker goroutines.
// It is generic over the input type T and the output type Q.
type Batcher[T any, Q any] struct {
	// config holds the BatchFunc and common arguments for processing.
	config BatchProcessorConfig[T, Q]
	// itemChannel is an unbuffered channel where new items are submitted.
	itemChannel chan BatchItem[T, Q]
	// maxBatchSize is the maximum number of items that can be included in a single batch.
	maxBatchSize int
	// batchInterval is the maximum duration to wait before processing a batch,
	// even if it hasn't reached maxBatchSize.
	batchInterval time.Duration
	// workerPool is a buffered channel used as a semaphore to limit the number
	// of concurrently running batch processing goroutines.
	workerPool chan struct{}
	// stopChan is used to signal the Batcher's runCollector goroutine to terminate.
	stopChan chan struct{}
	// stopped is a flag that indicates whether the Stop() method has been called,
	// preventing new submissions.
	stopped bool

	// batchManager handles batches for both sharded and non-sharded modes.
	// Cleanup logic is only enabled when sharding is used (KeyFunc != nil).
	batchManager *BatchManager[T, Q]
}

// NewBatcher creates and initializes a new Batcher instance.
// It starts a background goroutine (runCollector) to manage batch collection and dispatch.
//
// Parameters:
//
//	config:        The BatchProcessorConfig containing the BatchFunc and common arguments.
//	batchInterval: The time duration after which a pending batch is processed, even if not full.
//	maxBatchSize:  The maximum number of items to collect before processing a batch.
//	numWorkers:    The number of concurrent worker goroutines to process batches.
//	               If less than or equal to 0, it defaults to 1.
//
// Returns a pointer to the newly created Batcher.
func NewBatcher[T any, Q any](config BatchProcessorConfig[T, Q], batchInterval time.Duration, maxBatchSize int, numWorkers int) *Batcher[T, Q] {
	if numWorkers <= 0 {
		numWorkers = 1 // Default to at least one worker.
	}
	if maxBatchSize <= 0 {
		maxBatchSize = 1 // Default to at least one item per batch.
	}

	b := &Batcher[T, Q]{
		config:        config,
		itemChannel:   make(chan BatchItem[T, Q]), // Unbuffered channel for incoming items.
		maxBatchSize:  maxBatchSize,
		batchInterval: batchInterval,
		workerPool:    make(chan struct{}, numWorkers), // Semaphore for worker concurrency.
		stopChan:      make(chan struct{}),             // Signal channel for stopping.
	}

	// Always initialize batch manager, with cleanup enabled only if KeyFunc is provided
	cleanupEnabled := config.KeyFunc != nil
	b.batchManager = NewBatchManager[T, Q](cleanupEnabled, config.CleanupConfig)

	go b.runCollector() // Start the main batch collection loop.
	return b
}

// runCollector is the main loop for the Batcher. It runs in a separate goroutine.
// It collects items from itemChannel and forms batches based on maxBatchSize or batchInterval.
// It also handles graceful shutdown when stopChan is closed.
func (b *Batcher[T, Q]) runCollector() {
	ticker := time.NewTicker(b.batchInterval)
	defer ticker.Stop()

	for {
		select {
		case <-b.stopChan: // Signal to stop the batcher.
			// Process any remaining items in all batches.
			allBatches := b.batchManager.FlushAllBatches()
			for _, batch := range allBatches {
				if len(batch) > 0 {
					b.dispatchBatchProcessing(batch)
				}
			}
			b.batchManager.Stop()

			// Wait for all active workers to finish by trying to fill the workerPool.
			for i := 0; i < cap(b.workerPool); i++ {
				b.workerPool <- struct{}{}
			}
			close(b.workerPool)
			return

		case <-ticker.C: // batchInterval has elapsed.
			allBatches := b.batchManager.FlushAllBatches()
			for _, batch := range allBatches {
				if len(batch) > 0 {
					b.dispatchBatchProcessing(batch)
				}
			}

		case item, ok := <-b.itemChannel:
			if !ok {
				continue
			}

			// Determine the key for batching
			var key string
			if b.config.KeyFunc != nil {
				key = b.config.KeyFunc(item.Input)
			} else {
				key = "" // Use empty string for non-sharded mode
			}

			// Add item to batch manager and check if batch is ready
			readyBatch := b.batchManager.AddItem(key, item, b.maxBatchSize)
			if readyBatch != nil {
				// Batch is ready due to size - process it and reset the ticker
				b.dispatchBatchProcessing(readyBatch)

				// Reset the ticker to avoid unnecessary flushes too soon after this batch
				ticker.Reset(b.batchInterval)
			}
		}
	}
}

// dispatchBatchProcessing acquires a worker slot from the workerPool and starts a new
// goroutine to process the given batch of items.
// The batchToProcess slice MUST be a copy that is safe for the new goroutine to use
// without interference from the runCollector goroutine.
func (b *Batcher[T, Q]) dispatchBatchProcessing(batchToProcess []BatchItem[T, Q]) {
	if len(batchToProcess) == 0 {
		return // Do not dispatch empty batches.
	}

	b.workerPool <- struct{}{} // Acquire a worker slot (blocks if pool is full).

	go func() { // Worker goroutine.
		defer func() {
			// Release the worker slot.
			// This assumes workerPool is not closed while workers might still be releasing.
			// The Stop() logic should ensure workerPool is closed only after all workers are done.
			<-b.workerPool
		}()
		b.processBatch(batchToProcess) // Process the items.
	}()
}

// processBatch executes the user-defined BatchFunc with the items in the batch.
// It handles sending results or errors back to the respective channels of each BatchItem.
// It also recovers from panics in the BatchFunc and ensures item channels are closed.
func (b *Batcher[T, Q]) processBatch(batch []BatchItem[T, Q]) {
	panicked := true // Assume a panic might occur until BatchFunc completes successfully.
	defer func() {
		// This defer block ensures that:
		// 1. Panics from BatchFunc are recovered.
		// 2. If a panic occurred, an error is sent to all items in the batch.
		// 3. Output and Error channels for all items in this batch are closed,
		//    signaling completion to callers of SubmitAndAwait.
		if r := recover(); r != nil {
			log.Printf("Sluice.Batcher: Recovered from panic during BatchFunc execution: %v", r)
			if panicked { // If panic happened before BatchFunc returned or handled its own errors.
				batchErr := fmt.Errorf("sluice.Batcher: panic during batch processing: %v", r)
				for _, item := range batch {
					select {
					case item.Error <- batchErr:
					default:
					} // Non-blocking send.
					select {
					case item.Output <- *new(Q):
					default:
					} // Send zero value for output.
				}
			}
		}
		// Always close the item's channels to unblock SubmitAndAwait,
		// regardless of success, error, or panic.
		for _, item := range batch {
			close(item.Output)
			close(item.Error)
		}
	}()

	if len(batch) == 0 { // Should ideally be caught by dispatchBatchProcessing.
		panicked = false
		return
	}

	// Prepare inputs for the BatchFunc.
	inputs := make([]T, len(batch))
	for i, item := range batch {
		inputs[i] = item.Input
	}

	// Execute the user's batch processing function.
	outputs, batchErr := b.config.Func(inputs, b.config.CommonArgs...)
	panicked = false // BatchFunc completed (either successfully or returned an error).

	if batchErr != nil {
		// BatchFunc returned a batch-level error. Propagate this error to all items.
		log.Printf("Sluice.Batcher: Error during BatchFunc execution: %v", batchErr)
		for _, item := range batch {
			item.Error <- batchErr
			item.Output <- *new(Q) // Send zero value for output on batch error.
		}
		return // Results processing is skipped.
	}

	// BatchFunc executed successfully, distribute results to individual items.
	for _, item := range batch {
		if result, ok := outputs[item.ID]; ok {
			item.Output <- result
			item.Error <- nil // Explicitly send nil error for success.
		} else {
			// No output found for this item's ID in the BatchFunc's results.
			item.Error <- fmt.Errorf("sluice.Batcher: no output found for ID %s", item.ID)
			item.Output <- *new(Q) // Send zero value.
		}
	}
}

// SubmitAndAwait adds an item to the Batcher and blocks until the item has been
// processed and its result (or an error) is available.
//
// Parameters:
//
//	id:    A unique string identifier for this item. This ID is used by the BatchFunc
//	       to map its results back to the specific item.
//	input: The input data of type T for this item.
//
// Returns the processed output of type Q and an error if one occurred during processing
// or if the Batcher is stopped.
func (b *Batcher[T, Q]) SubmitAndAwait(id string, input T) (Q, error) {
	if b.stopped {
		return *new(Q), fmt.Errorf("sluice.Batcher: Batcher has been stopped, not accepting new items")
	}

	// Create dedicated channels for this item's result and error.
	outputChan := make(chan Q, 1)    // Buffered to allow processBatch to send without blocking.
	errorChan := make(chan error, 1) // Buffered for the same reason.

	itemToSubmit := BatchItem[T, Q]{
		ID:     id,
		Input:  input,
		Output: outputChan,
		Error:  errorChan,
	}

	defer func() {
		// This defer handles the case where sending to b.itemChannel panics
		// (e.g., if b.itemChannel is closed by Stop() concurrently).
		if r := recover(); r != nil {
			log.Printf("Sluice.Batcher: Recovered from panic in SubmitAndAwait (likely send on closed itemChannel): %v", r)
			// If the item was not successfully sent to itemChannel, its output/error channels
			// will not be closed by processBatch. We must close them here to prevent the caller
			// of SubmitAndAwait from blocking indefinitely.
			select {
			case errorChan <- fmt.Errorf("sluice.Batcher: Batcher is stopping or stopped, submission failed: %v", r):
			default: // Avoid blocking if channel is already full or closed.
			}
			select {
			case outputChan <- *new(Q): // Send zero value.
			default:
			}
			// Ensure these specific channels are closed if this panic path is taken.
			close(outputChan)
			close(errorChan)
		}
	}()

	b.itemChannel <- itemToSubmit // Submit the item; this may panic if channel is closed.

	// Wait for the result or an error from the item's dedicated channels.
	// These channels will be closed by processBatch after sending values or upon error/panic.
	var q Q
	var errResult error

	// Read from channels. The order doesn't strictly matter due to buffering and closure.
	qResult, qOk := <-outputChan
	errVal, errOk := <-errorChan

	if qOk {
		q = qResult
	}
	if errOk {
		errResult = errVal
	}

	return q, errResult
}

// Stop signals the Batcher to cease accepting new items and to process any items
// already submitted or currently in its internal batch. It then shuts down its
// worker goroutines gracefully. This method is idempotent.
func (b *Batcher[T, Q]) Stop() {
	b.stopped = true     // Set flag to reject new SubmitAndAwait calls immediately.
	close(b.stopChan)    // Signal runCollector to initiate shutdown.
	close(b.itemChannel) // Prevent new items from being enqueued and unblocks runCollector's select.
	// The runCollector goroutine will handle processing remaining items and then
	// ensure all worker goroutines complete before closing b.workerPool and exiting.
}

// runCleanup runs in a separate goroutine and periodically sends cleanup requests
// to the main collector goroutine via a channel, avoiding the need for locks.
