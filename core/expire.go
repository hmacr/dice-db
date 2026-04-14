package core

import (
	"time"
)

func hasExpired(obj *Obj) bool {
	exp, ok := expires[obj]
	if !ok {
		return false
	}
	return exp <= uint64(time.Now().UnixMilli())
}

func getExpiry(obj *Obj) (uint64, bool) {
	exp, ok := expires[obj]
	return exp, ok
}

func expireSample() float32 {
	var limit int = 20
	var expiredCount int = 0

	// Assuming iteration of golang hash table is randomized
	for key, obj := range store {
		limit--
		if hasExpired(obj) {
			Del(key)
			expiredCount++
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
}
