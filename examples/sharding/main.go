package main

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/karangupta31/go-sluice"
)

// Request represents a request that needs to be batched
type Request struct {
	ExperimentID string
	UserID       string
}

// Response represents the response for a request
type Response struct {
	ExperimentID string
	UserID       string
	Variant      string
}

func main() {
	fmt.Println("=== Sluice Sharding Example ===")

	// 1. Define your batch processing function
	// This simulates calling an API that takes multiple users for the same experiment
	batchFetchVariants := func(requests []Request, commonArgs ...interface{}) (map[string]Response, error) {
		fmt.Printf("BatchFunc: Processing batch of %d requests for experiment: %s\n",
			len(requests), requests[0].ExperimentID)

		// Simulate API call latency
		time.Sleep(100 * time.Millisecond)

		results := make(map[string]Response)
		for i, req := range requests {
			// Create unique ID for each request
			id := fmt.Sprintf("%s-%s", req.ExperimentID, req.UserID)

			// Simulate different variants for different users
			variant := fmt.Sprintf("variant_%d", (i%3)+1)

			results[id] = Response{
				ExperimentID: req.ExperimentID,
				UserID:       req.UserID,
				Variant:      variant,
			}
		}
		return results, nil
	}

	// 2. Define the sharding key function
	// This groups requests by experiment ID, so all requests for the same experiment
	// are batched together (which is more efficient for the downstream API)
	keyFunc := func(req Request) string {
		return req.ExperimentID // Group by experiment ID
	}

	// 3. Create BatchProcessorConfig with sharding
	config := sluice.BatchProcessorConfig[Request, Response]{
		Func:    batchFetchVariants,
		KeyFunc: keyFunc, // Enable sharding by experiment ID
	}

	// 4. Create batcher
	batcher := sluice.NewBatcher(
		config,
		200*time.Millisecond, // Batch interval
		5,                    // Max batch size per experiment
		3,                    // Max concurrent workers
	)
	defer batcher.Stop()

	// 5. Simulate requests for different experiments
	requests := []Request{
		{"exp_123", "user_1"},
		{"exp_123", "user_2"},
		{"exp_123", "user_3"}, // These 3 should be batched together
		{"exp_456", "user_4"},
		{"exp_456", "user_5"}, // These 2 should be in a separate batch
		{"exp_789", "user_6"}, // This should be in its own batch
	}

	var wg sync.WaitGroup
	fmt.Println("\nSubmitting requests:")

	for _, req := range requests {
		wg.Add(1)
		go func(r Request) {
			defer wg.Done()

			// Create unique ID for correlation
			id := fmt.Sprintf("%s-%s", r.ExperimentID, r.UserID)

			fmt.Printf("Submitting: %s\n", id)

			response, err := batcher.SubmitAndAwait(id, r)
			if err != nil {
				log.Printf("Error for %s: %v\n", id, err)
				return
			}

			fmt.Printf("Received response for %s: %+v\n", id, response)
		}(req)

		// Small delay to show batching behavior
		time.Sleep(10 * time.Millisecond)
	}

	wg.Wait()
	fmt.Println("\n=== All requests completed ===")

	// Notice in the output:
	// - Requests for exp_123 are processed together in one batch
	// - Requests for exp_456 are processed together in another batch
	// - Request for exp_789 is processed in its own batch
	// This is much more efficient than calling the API separately for each request!
}
