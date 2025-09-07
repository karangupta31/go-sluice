package sluice

import (
	"fmt"
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
	triggerChannel := make(chan string, 100) // Buffered channel for batch ready notifications
	b.batchManager = NewBatchManager[T, Q](cleanupEnabled, config.CleanupConfig, triggerChannel, batchInterval, maxBatchSize)

	go b.runCollector(triggerChannel) // Start the main batch collection loop.
	return b
}

// runCollector is the main loop for the Batcher. It runs in a separate goroutine.
// It collects items from itemChannel and forms batches based on maxBatchSize or batchInterval.
// It also handles graceful shutdown when stopChan is closed.
// Prioritizes stop channel, then trigger channel processing to prevent batch size overflow.
func (b *Batcher[T, Q]) runCollector(triggerChannel chan string) {
	for {
		// Don't remove seperate select for various cases.
		// Select uses psuedo random order while selecting cases, but we need definite order
		// PRIORITY 1: Check for shutdown first
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

			// Close itemChannel to prevent new sends
			close(b.itemChannel)

			// Wait for all active workers to finish by trying to fill the workerPool.
			for i := 0; i < cap(b.workerPool); i++ {
				b.workerPool <- struct{}{}
			}
			close(b.workerPool)
			return
		default:
		}

		// PRIORITY 2: Process triggers to prevent batch overflow
		select {
		case batchKey := <-triggerChannel:
			// A batch is ready (either by size or timer)
			batch := b.batchManager.FlushBatch(batchKey)
			if len(batch) > 0 {
				b.dispatchBatchProcessing(batch)
			}
			continue // Go back to start to check stop and any other triggers
		default:
		}

		// PRIORITY 3: Only process items if no triggers are pending
		select {
		case item, ok := <-b.itemChannel:
			if !ok {
				// itemChannel was closed, this means we're shutting down
				continue
			}

			// Determine the key for batching
			var key string
			if b.config.KeyFunc != nil {
				key = b.config.KeyFunc(item.Input)
			} else {
				key = "" // Use empty string for non-sharded mode
			}

			// Add item to batch manager - it handles timer management internally
			b.batchManager.AddItem(key, item)
		default:
			// No items available, continue to next iteration
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

func (b *Batcher[T, Q]) processBatch(batch []BatchItem[T, Q]) {
	var panicErr error

	defer func() {
		// Handle panic and send error to all items
		if r := recover(); r != nil {
			panicErr = fmt.Errorf("sluice.Batcher: panic during batch processing: %v", r)
			for _, item := range batch {
				item.Error <- panicErr
				item.Output <- *new(Q)
			}
		}

		// Always close channels last
		for _, item := range batch {
			close(item.Output)
			close(item.Error)
		}
	}()

	if len(batch) == 0 {
		return
	}

	// Prepare inputs
	inputs := make([]T, len(batch))
	for i, item := range batch {
		inputs[i] = item.Input
	}

	// Execute batch function
	outputs, err := b.config.Func(inputs, b.config.CommonArgs...)

	if err != nil {
		// Batch-level error
		for _, item := range batch {
			item.Error <- err
			item.Output <- outputs[item.ID]
		}
		return
	}

	// Distribute results
	for _, item := range batch {
		if result, ok := outputs[item.ID]; ok {
			item.Output <- result
			item.Error <- nil
		} else {
			item.Error <- fmt.Errorf("sluice.Batcher: no output found for ID %s", item.ID)
			item.Output <- outputs[item.ID]
		}
	}
}

func (b *Batcher[T, Q]) SubmitAndAwait(id string, input T) (Q, error) {
	// Check if stop channel is closed instead of using stopped flag
	select {
	case <-b.stopChan:
		return *new(Q), fmt.Errorf("sluice.Batcher: Batcher has been stopped, not accepting new items")
	default:
	}

	outputChan := make(chan Q, 1)
	errorChan := make(chan error, 1)

	item := BatchItem[T, Q]{
		ID:     id,
		Input:  input,
		Output: outputChan,
		Error:  errorChan,
	}

	// Try to send item, but return error if batcher is stopped
	select {
	case b.itemChannel <- item:
		// Successfully sent
	case <-b.stopChan:
		return *new(Q), fmt.Errorf("sluice.Batcher: Batcher has been stopped, not accepting new items")
	}

	// Wait for result
	result := <-outputChan
	err := <-errorChan

	return result, err
}

// Stop signals the Batcher to cease accepting new items and to process any items
// already submitted or currently in its internal batch. It then shuts down its
// worker goroutines gracefully. This method is idempotent.
func (b *Batcher[T, Q]) Stop() {
	// Close stopChan first to signal shutdown
	select {
	case <-b.stopChan:
		// Already stopped
		return
	default:
		close(b.stopChan) // Signal runCollector to initiate shutdown
		// Don't close itemChannel - let runCollector handle draining
	}
	// The runCollector goroutine will handle processing remaining items and then
	// ensure all worker goroutines complete before closing b.workerPool and exiting.
}
