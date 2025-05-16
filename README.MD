# Go Sluice - Batch Processor

[![Go Reference](https://pkg.go.dev/badge/github.com/karangupta31/go-sluice.svg)](https://pkg.go.dev/github.com/karangupta31/go-sluice)

`go-sluice` provides a generic, concurrent batch processing utility for Go, available under the `sluice` package. It allows collecting individual items (like user IDs for fetching details) and processing them in controlled batches. This is useful for optimizing operations like database queries or API calls by grouping multiple small requests into fewer, larger ones.

The `sluice.Batcher` is the core component, designed for ease of use and efficient, concurrent batch execution.

## Features

*   **Generic:** Works with any input (`T`) and output (`Q`) types using Go 1.18+ generics.
*   **Concurrent:** Processes multiple batches concurrently using a configurable worker pool.
*   **Configurable Batching:** Triggers batch processing based on:
    *   Maximum batch size.
    *   Time interval (batch window).
*   **ID Correlation:** Submitted items are given an ID, used by the `BatchFunc` to map results back, ensuring you get the correct output for your input.
*   **Synchronous Submission:** The `SubmitAndAwait` method provides a simple way to submit an item and block until its specific result is available.
*   **Graceful Shutdown:** The `Stop()` method ensures that any enqueued items are processed before the batcher shuts down its worker goroutines.
*   **Panic Safe:** Recovers from panics within the user-provided `BatchFunc`, logging the error and ensuring the batcher itself remains operational.

## Installation

```bash
go get github.com/karangupta31/go-sluice
```
## Usage
```go
package main

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/karangupta31/go-sluice/sluice"
)

// User represents the data structure for user details.
type User struct {
	ID   string
	Name string
	// Add more fields like Email, Age, etc.
}

func main() {
	// 1. Define your batch processing function (BatchFunc)
	// This function takes a slice of user IDs (inputs) and returns a map of User objects.
	batchFetchUserDetails := func(userIDs []string, commonArgs ...interface{}) (map[string]User, error) {
		fmt.Printf("BatchFunc: Processing batch of %d user IDs: %v\n", len(userIDs), userIDs)

		// Simulate fetching data from a database or an API.
		// In a real application, you might do:
		// users, err := db.GetUsersByIDs(userIDs)
		time.Sleep(50 * time.Millisecond) // Simulate I/O latency

		results := make(map[string]User)
		for _, id := range userIDs {
			// The key in the 'results' map MUST match the ID provided to SubmitAndAwait.
			results[id] = User{
				ID:   id,
				Name: "User-" + id, // Example: "User-user123"
			}
		}
		// If a critical error occurs for the whole batch (e.g., DB connection lost), return it.
		// return nil, fmt.Errorf("database connection failed")
		return results, nil
	}

	// 2. Create BatchProcessorConfig
	// This holds your BatchFunc and any common arguments (e.g., a database connection pool).
	processorConfig := sluice.BatchProcessorConfig[string, User]{
		Func: batchFetchUserDetails,
		// CommonArgs: []interface{}{dbPool}, // Optional: for passing shared resources
	}

	// 3. Create a new Batcher instance
	batcher := sluice.NewBatcher(
		processorConfig,
		100*time.Millisecond, // Process batch every 100ms if not full (batch window)
		10,                   // Max 10 user IDs per batch (max batch size)
		5,                    // Max 5 concurrent workers processing batches
	)
	// Ensure graceful shutdown when main exits or Batcher is no longer needed.
	defer batcher.Stop()

	// 4. Submit requests and await results
	// Example: Fetching details for a few users.
	userIDsToFetch := []string{"user123", "user456", "user789", "user000"}
	var wg sync.WaitGroup

	fmt.Println("Submitting user IDs for batch fetching:")
	for _, userID := range userIDsToFetch {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			fmt.Printf("Submitting: %s\n", id)
			// SubmitAndAwait blocks until this specific item is processed.
			// The first argument is the ID for correlation, the second is the input data.
			user, err := batcher.SubmitAndAwait(id, id)
			if err != nil {
				log.Printf("Error fetching user %s: %v\n", id, err)
				return
			}
			fmt.Printf("Received details for %s: %+v\n", id, user)
		}(userID)
	}
	wg.Wait() // Wait for all goroutines to complete.

	fmt.Println("\nAll user details fetched.")
	// Allow time for any final log messages or Stop() to complete.
	time.Sleep(200 * time.Millisecond)
}
```