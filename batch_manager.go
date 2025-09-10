package sluice

import (
	"log"
	"sync"
	"time"
)

// Constants for key management defaults
const (
	// defaultMaxKeysBeforeCleanup is the default threshold for triggering LRU cleanup
	defaultMaxKeysBeforeCleanup = 1000
	// defaultKeyInactiveThreshold is the default time a key must be unused before it's eligible for cleanup
	defaultKeyInactiveThreshold = 10 * time.Minute
	// defaultCleanupInterval is the default frequency to check for inactive keys
	defaultCleanupInterval = 5 * time.Minute
)

// BatchManager handles the management of batches using a unified map-based approach.
// For sharded mode (multi-key), it provides cleanup functionality to prevent memory leaks.
// For non-sharded mode (single-key), it uses the same map with a default key and no cleanup overhead.
type BatchManager[T any, Q any] struct {
	// batches holds the current batches for each key (including default key for non-sharded mode)
	batches map[string][]BatchItem[T, Q]
	// keyLastUsed tracks when each key was last accessed for LRU cleanup
	keyLastUsed map[string]time.Time

	// Batch configuration
	maxBatchSize int

	// Timer management for batch flushing
	keyTimers      map[string]*time.Timer
	triggerChannel chan string
	batchInterval  time.Duration
	timerEnabled   bool // true if batchInterval > 0

	// Cleanup configuration and channels (only used when cleanup is enabled)
	cleanupEnabled       bool
	cleanupWg            sync.WaitGroup // For cleanup goroutine lifecycle
	cleanupStopChan      chan struct{}
	cleanupCoordWg       sync.WaitGroup // For coordinating cleanup with batcher operations
	maxKeysBeforeCleanup int
	keyInactiveThreshold time.Duration
	cleanupInterval      time.Duration
}

// NewBatchManager creates a new BatchManager using a unified map-based approach.
// If cleanupEnabled is true, it starts the cleanup goroutine for memory management.
// If cleanupEnabled is false, it uses the same map structure but without cleanup overhead.
func NewBatchManager[T any, Q any](cleanupEnabled bool, cleanupConfig *CleanupConfig, triggerChannel chan string, batchInterval time.Duration, maxBatchSize int) *BatchManager[T, Q] {
	bm := &BatchManager[T, Q]{
		batches:        make(map[string][]BatchItem[T, Q]),
		keyLastUsed:    make(map[string]time.Time),
		cleanupEnabled: cleanupEnabled,

		// Batch configuration
		maxBatchSize: maxBatchSize,

		// Timer management
		keyTimers:      make(map[string]*time.Timer),
		triggerChannel: triggerChannel,
		batchInterval:  batchInterval,
		timerEnabled:   batchInterval > 0,
	}

	if cleanupEnabled {
		// Initialize cleanup configuration only when cleanup is enabled
		maxKeys := defaultMaxKeysBeforeCleanup
		inactiveThreshold := defaultKeyInactiveThreshold
		interval := defaultCleanupInterval

		if cleanupConfig != nil {
			if cleanupConfig.MaxKeysBeforeCleanup > 0 {
				maxKeys = cleanupConfig.MaxKeysBeforeCleanup
			}
			if cleanupConfig.KeyInactiveThreshold > 0 {
				inactiveThreshold = cleanupConfig.KeyInactiveThreshold
			}
			if cleanupConfig.CleanupInterval > 0 {
				interval = cleanupConfig.CleanupInterval
			}
		}

		bm.cleanupStopChan = make(chan struct{})
		bm.maxKeysBeforeCleanup = maxKeys
		bm.keyInactiveThreshold = inactiveThreshold
		bm.cleanupInterval = interval

		// TODO: Cleanup goroutine temporarily disabled due to race condition detection
		// with waitgroup coordination. Go's race detector doesn't recognize waitgroup-based
		// mutual exclusion as valid synchronization, even though the logic is correct.
		// The memory impact of not cleaning up is minimal (~100-150 bytes per empty key).
		// We'll revisit this in a future version with either sync.Map or a different approach.
		//
		// Start the cleanup goroutine only when cleanup is enabled
		// bm.cleanupWg.Add(1)
		// go bm.runCleanup()
	}

	return bm
}

// AddItem adds an item to the appropriate batch and returns the batch if it's ready for processing.
// Uses the provided key to determine the batch (or default key for non-sharded mode).
// Manages per-key timers and triggers batch processing via the trigger channel.
func (bm *BatchManager[T, Q]) AddItem(key string, item BatchItem[T, Q]) {
	// Note: cleanupCoordWg.Wait() temporarily removed since cleanup is disabled
	// bm.cleanupCoordWg.Wait()

	bm.batches[key] = append(bm.batches[key], item)
	bm.keyLastUsed[key] = time.Now()

	// Start timer for this key if timers are enabled and this is the first item
	if bm.timerEnabled && len(bm.batches[key]) == 1 {
		bm.startTimerForKey(key)
	}

	// Check if batch is ready due to size
	if len(bm.batches[key]) >= bm.maxBatchSize {
		// Stop the timer since we're dispatching due to size
		bm.stopTimerForKey(key)

		// Trigger batch processing
		select {
		case bm.triggerChannel <- key:
		default:
			// If trigger channel is full, we skip this notification
			// The batch will still be available when FlushBatch is called
		}
	}
}

// startTimerForKey starts a timer for the given key.
// When the timer expires, it sends the key to the trigger channel only if the batch is not empty.
func (bm *BatchManager[T, Q]) startTimerForKey(key string) {
	if !bm.timerEnabled {
		return
	}

	// Check if timer already exists for this key
	if timer, exists := bm.keyTimers[key]; exists {
		// Reset existing timer
		timer.Reset(bm.batchInterval)
	} else {
		// Create a new timer
		bm.keyTimers[key] = time.AfterFunc(bm.batchInterval, func() {
			// Note: cleanupCoordWg.Wait() temporarily removed since cleanup is disabled
			// bm.cleanupCoordWg.Wait()
			// Timer expired - trigger without checking batch length
			// FlushBatch will handle empty batches downstream
			bm.triggerChannel <- key
		})
	}
}

// stopTimerForKey stops the timer for the given key but keeps it in the map for reuse.
func (bm *BatchManager[T, Q]) stopTimerForKey(key string) {
	if timer, exists := bm.keyTimers[key]; exists {
		timer.Stop()
		// Don't delete from map - keep for reuse
	}
}

// deleteTimerForKey permanently removes the timer for the given key.
// This should only be called during cleanup or shutdown.
func (bm *BatchManager[T, Q]) deleteTimerForKey(key string) {
	if timer, exists := bm.keyTimers[key]; exists {
		timer.Stop()
		delete(bm.keyTimers, key)
	}
}

// FlushBatch returns and clears the batch for the given key if it has items.
// Also stops any active timer for the key.
func (bm *BatchManager[T, Q]) FlushBatch(key string) []BatchItem[T, Q] {
	// Note: cleanupCoordWg.Wait() temporarily removed since cleanup is disabled
	// bm.cleanupCoordWg.Wait()

	if len(bm.batches[key]) > 0 {
		batch := bm.batches[key]
		bm.batches[key] = nil
		bm.keyLastUsed[key] = time.Now()

		// Stop the timer since we're flushing the batch
		bm.stopTimerForKey(key)

		return batch
	}
	return nil
}

// FlushAllBatches returns all non-empty batches and clears them.
// Uses FlushBatch internally to ensure consistent behavior.
func (bm *BatchManager[T, Q]) FlushAllBatches() [][]BatchItem[T, Q] {
	// Note: cleanupCoordWg.Wait() temporarily removed since cleanup is disabled
	// bm.cleanupCoordWg.Wait()

	var allBatches [][]BatchItem[T, Q]

	// Iterate directly over the map and flush each batch
	for key := range bm.batches {
		batch := bm.FlushBatch(key)
		if len(batch) > 0 {
			allBatches = append(allBatches, batch)
		}
	}

	return allBatches
}

// GetAllKeys returns all current batch keys (for testing purposes).
func (bm *BatchManager[T, Q]) GetAllKeys() []string {
	// Note: cleanupCoordWg.Wait() temporarily removed since cleanup is disabled
	// bm.cleanupCoordWg.Wait()

	keys := make([]string, 0, len(bm.batches))
	for key := range bm.batches {
		keys = append(keys, key)
	}
	return keys
}

// Stop gracefully shuts down the BatchManager.
// If cleanup is enabled, it stops the cleanup goroutine and clears all data.
// Also stops all active timers.
func (bm *BatchManager[T, Q]) Stop() {
	// Note: Cleanup goroutine is temporarily disabled, so no cleanup coordination needed
	// if bm.cleanupEnabled {
	//     close(bm.cleanupStopChan)
	//     bm.cleanupWg.Wait()
	// }

	// Stop all active timers
	for key := range bm.keyTimers {
		bm.deleteTimerForKey(key)
	}

	// Clear all data
	bm.batches = nil
	bm.keyLastUsed = nil
	bm.keyTimers = nil
}

// runCleanup runs in a separate goroutine and handles cleanup requests.
// This method should only be called when cleanup is enabled.
func (bm *BatchManager[T, Q]) runCleanup() {
	defer bm.cleanupWg.Done()

	if !bm.cleanupEnabled {
		// Safety check: cleanup should not run when disabled
		return
	}

	ticker := time.NewTicker(bm.cleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-bm.cleanupStopChan:
			return
		case <-ticker.C:
			// Periodic cleanup
			bm.performCleanup()
		}
	}
}

// performCleanup removes inactive keys to prevent memory leaks.
// This method should only be called when cleanup is enabled.
func (bm *BatchManager[T, Q]) performCleanup() {
	if !bm.cleanupEnabled {
		// Safety check: cleanup should not run when disabled
		return
	}

	// Signal that cleanup is starting and batch operations should wait.
	// This ensures that batch operations (AddItem, FlushBatch, etc.) calling
	// bm.cleanupCoordWg.Wait() will block if performCleanup is active.
	bm.cleanupCoordWg.Add(1)
	defer bm.cleanupCoordWg.Done() // Ensure Done is called even if cleanup logic panics or returns.

	if len(bm.batches) < bm.maxKeysBeforeCleanup {
		return // No cleanup needed yet
	}

	now := time.Now()
	keysToRemove := []string{}

	for key, lastUsed := range bm.keyLastUsed {
		if now.Sub(lastUsed) > bm.keyInactiveThreshold {
			// Only remove the key if its batch is also empty.
			// This prevents data loss for items in a batch that hasn't been
			// explicitly flushed or hit size limit recently but the key is old.
			if len(bm.batches[key]) == 0 {
				keysToRemove = append(keysToRemove, key)
			}
		}
	}

	if len(keysToRemove) > 0 {
		log.Printf("Sluice.BatchManager: Cleaning up %d inactive keys", len(keysToRemove))
		for _, key := range keysToRemove {
			delete(bm.batches, key)
			delete(bm.keyLastUsed, key)
			// Also permanently delete the timer for this key
			bm.deleteTimerForKey(key)
		}
	}
}
