package main

import (
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/karangupta31/go-sluice/sluice"
)

// User represents the data structure for user details.
type User struct {
	ID   string
	Name string
	City string // Added another field for a slightly richer example
}

func main() {
	fmt.Println("Go Sluice - Batcher Example: User Profile Fetching")
	fmt.Println("-------------------------------------------------")

	// 1. Define your batch processing function (sluice.BatchFunc)
	// This function will receive a slice of inputs (user IDs in this case)
	// and should return a map of results keyed by the input ID.
	batchFetchUserProfiles := func(userIDs []string, args ...interface{}) (map[string]User, error) {
		// args can be used to pass common dependencies or configurations
		if len(args) > 0 {
			if apiEndpoint, ok := args[0].(string); ok {
				fmt.Printf("BatchFunc: Using common arg (e.g., API Endpoint): %s\n", apiEndpoint)
			}
		}

		fmt.Printf("BatchFunc: Processing batch of %d user IDs: %v\n", len(userIDs), userIDs)

		// Simulate fetching data from a database or an external API.
		// The latency might depend on the number of items in the batch.
		simulatedLatency := 50*time.Millisecond + time.Duration(len(userIDs)*5)*time.Millisecond
		fmt.Printf("BatchFunc: Simulating I/O latency of %v for %d items...\n", simulatedLatency, len(userIDs))
		time.Sleep(simulatedLatency)

		results := make(map[string]User)
		for i, id := range userIDs {
			// The key in the results map (id) MUST match the ID provided to SubmitAndAwait.
			city := "City-" + strconv.Itoa(i%3+1) // Assign cities cyclically for variety
			results[id] = User{
				ID:   id,
				Name: "User " + id, // Example: "User user123"
				City: city,
			}
		}
		fmt.Printf("BatchFunc: Finished processing %d user IDs.\n", len(userIDs))
		return results, nil
		// To simulate a batch-level error (e.g., API unavailable):
		// return nil, fmt.Errorf("external user API is currently unavailable")
	}

	// 2. Create the BatchProcessorConfig
	// This struct bundles your BatchFunc and any common arguments.
	processorConfig := sluice.BatchProcessorConfig[string, User]{
		Func:       batchFetchUserProfiles,
		CommonArgs: []interface{}{"https://api.example.com/users"}, // Example common argument
	}

	// 3. Create a new Batcher instance from the sluice package
	batchInterval := 150 * time.Millisecond // Process batch every 150ms if not full
	maxBatchSize := 8                       // Max 8 user IDs per batch
	numWorkers := 4                         // Up to 4 batches can be processed concurrently

	fmt.Printf("Initializing Batcher: Interval=%v, MaxSize=%d, Workers=%d\n", batchInterval, maxBatchSize, numWorkers)
	batcher := sluice.NewBatcher(
		processorConfig,
		batchInterval,
		maxBatchSize,
		numWorkers,
	)

	// defer batcher.Stop() ensures the batcher is shut down cleanly when main exits.
	defer func() {
		fmt.Println("\nMain: Initiating Batcher stop...")
		batcher.Stop()
		fmt.Println("Main: Batcher stopped.")
	}()

	// 4. Submit items to the batcher using SubmitAndAwait
	userIDsToFetch := []string{
		"alpha", "bravo", "charlie", "delta", "echo",
		"foxtrot", "golf", "hotel", "india", "juliett", // First batch (max 8), then 2
		"kilo", "lima", "mike", "november", "oscar", // Another batch of 5
		"papa", // Single item, likely batched by time
	}
	var wg sync.WaitGroup

	fmt.Printf("\nMain: Submitting %d user IDs for profile fetching...\n", len(userIDsToFetch))

	for i, userID := range userIDsToFetch {
		wg.Add(1)
		go func(id string, submissionOrder int) {
			defer wg.Done()
			// fmt.Printf("Goroutine %d: Submitting ID '%s'\n", submissionOrder, id)

			// The first argument to SubmitAndAwait is the ID for correlation.
			// The second argument is the actual input data (T type).
			profile, err := batcher.SubmitAndAwait(id, id)
			if err != nil {
				log.Printf("Goroutine %d: Error fetching profile for '%s': %v\n", submissionOrder, id, err)
				return
			}
			fmt.Printf("Goroutine %d: Received profile for '%s': Name='%s', City='%s'\n",
				submissionOrder, id, profile.Name, profile.City)
		}(userID, i+1)

		// Stagger submissions to see time-based batching more clearly for smaller sets
		if i%4 == 0 {
			time.Sleep(20 * time.Millisecond)
		} else {
			time.Sleep(50 * time.Millisecond)
		}
	}

	wg.Wait() // Wait for all goroutines (and thus their SubmitAndAwait calls) to complete.
	fmt.Println("\nMain: All SubmitAndAwait calls have returned.")

	// Give a bit of time for any final logging or ticker-based batches to clear
	// before the deferred Stop() is called.
	time.Sleep(300 * time.Millisecond)
	fmt.Println("\nMain: Example finished.")
}
