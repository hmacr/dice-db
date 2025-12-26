package core

import (
	"log"
	"time"
)

func expireSample() float32 {
	var limit int = 20
	var expiredCount int = 0

	// Assuming iteration of golang hash table is randomized
	for key, obj := range store {
		if obj.ExpiresAt != -1 {
			limit--
			// Key expired
			if obj.ExpiresAt <= time.Now().UnixMilli() {
				delete(store, key)
				expiredCount++
			}
		}

		// Once we iterated to 20 keys that have some expiration set,
		// we break the loop.
		if limit == 0 {
			break
		}
	}

	return float32(expiredCount) / float32(20.0)
}

// Deletes all the expired keys - the active way
// Sampling approach
func DeleteExpiredKeys() {
	for {
		frac := expireSample()

		// If the sample had less than 25% keys expires, we break the loop.
		if frac < 0.25 {
			break
		}
	}
	log.Printf("deleted the expired but undeleted keys.. total keys remaining = %d\n", len(store))
}
