package core

// TODO: Change ExpiresAt to LRU bits as handled by Redis
type Obj struct {
	TypeEncoding uint8
	Value        any
	// Redis allots 24 bits to this item, but we will use 32 bits because
	// Golang does not support bitfields and we need not make this super-complicated
	// by mergin TypeEncoding + LastAccessedAt in one 32-bit integer.
	// But nonetheless, we can benchmark and see how that fares.
	// For now, we continue with 32 bit integer to store `LastAccessedAt`
	LastAccessedAt uint32
}

// Types
var OBJ_TYPE_STRING uint8 = 0 << 4

// Encondings
var OBJ_ENCODING_RAW uint8 = 0
var OBJ_ENCODING_INT uint8 = 1
var OBJ_ENCODING_EMBSTR uint8 = 8
