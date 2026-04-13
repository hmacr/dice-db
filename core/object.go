package core

// TODO: Change ExpiresAt to LRU bits as handled by Redis
type Obj struct {
	TypeEncoding uint8
	Value        any
	ExpiresAt    int64
}

// Types
var OBJ_TYPE_STRING uint8 = 0 << 4

// Encondings
var OBJ_ENCODING_RAW uint8 = 0
var OBJ_ENCODING_INT uint8 = 1
var OBJ_ENCODING_EMBSTR uint8 = 8
