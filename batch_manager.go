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
func NewBatchManager[T any, Q any](cleanupEnabled bool, cleanupConfig *CleanupConfig) *BatchManager[T, Q] {
	bm := &BatchManager[T, Q]{
		batches:        make(map[string][]BatchItem[T, Q]),
		keyLastUsed:    make(map[string]time.Time),
		cleanupEnabled: cleanupEnabled,
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

		// Start the cleanup goroutine only when cleanup is enabled
		bm.cleanupWg.Add(1)
		go bm.runCleanup()
	}

	return bm
}

// AddItem adds an item to the appropriate batch and returns the batch if it's ready for processing.
// Uses the provided key to determine the batch (or default key for non-sharded mode).
func (bm *BatchManager[T, Q]) AddItem(key string, item BatchItem[T, Q], maxBatchSize int) []BatchItem[T, Q] {
	// Wait if cleanup is active (only blocks when cleanup is running)
	bm.cleanupCoordWg.Wait()

	bm.batches[key] = append(bm.batches[key], item)
	bm.keyLastUsed[key] = time.Now()

	if len(bm.batches[key]) >= maxBatchSize {
		batch := bm.batches[key]
		bm.batches[key] = nil
		return batch
	}
	return nil
}

// FlushBatch returns and clears the batch for the given key if it has items.
func (bm *BatchManager[T, Q]) FlushBatch(key string) []BatchItem[T, Q] {
	// Wait if cleanup is active (only blocks when cleanup is running)
	bm.cleanupCoordWg.Wait()

	if len(bm.batches[key]) > 0 {
		batch := bm.batches[key]
		bm.batches[key] = nil
		bm.keyLastUsed[key] = time.Now()
		return batch
	}
	return nil
}

// FlushAllBatches returns all non-empty batches and clears them.
func (bm *BatchManager[T, Q]) FlushAllBatches() [][]BatchItem[T, Q] {
	// Wait if cleanup is active (only blocks when cleanup is running)
	bm.cleanupCoordWg.Wait()

	var allBatches [][]BatchItem[T, Q]
	now := time.Now()

	for key, batch := range bm.batches {
		if len(batch) > 0 {
			allBatches = append(allBatches, batch)
			bm.batches[key] = nil
			bm.keyLastUsed[key] = now
		}
	}
	return allBatches
}

// GetAllKeys returns all current batch keys (for testing purposes).
func (bm *BatchManager[T, Q]) GetAllKeys() []string {
	// Wait if cleanup is active (only blocks when cleanup is running)
	bm.cleanupCoordWg.Wait()

	keys := make([]string, 0, len(bm.batches))
	for key := range bm.batches {
		keys = append(keys, key)
	}
	return keys
}

// Stop gracefully shuts down the BatchManager.
// If cleanup is enabled, it stops the cleanup goroutine and clears all data.
func (bm *BatchManager[T, Q]) Stop() {
	if bm.cleanupEnabled {
		// Stop cleanup goroutine and clear maps
		close(bm.cleanupStopChan)
		bm.cleanupWg.Wait()
	}

	// Clear all data
	bm.batches = nil
	bm.keyLastUsed = nil
}

// runCleanup runs in a separate goroutine and handles cleanup requests.
// This method should only be called when cleanup is enabled.
func (bm *BatchManager[T, Q]) runCleanup() {
	defer bm.cleanupWg.Done()

	if !bm.cleanupEnabled {
		// Safety check: cleanup should not run when disabled
		log.Printf("Sluice.BatchManager: Warning - cleanup goroutine started when cleanup is disabled")
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
		}
	}
}
