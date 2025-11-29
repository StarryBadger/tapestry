package id

import (
	"bytes"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"math/big"
	"math/rand"
	"time"
)

// TAPESTRY CONSTANTS
const (
	BITS        = 160
	BYTES       = BITS / 8
	DIGITS      = 40 // 160 bits / 4 bits per hex digit
	RADIX       = 16
	DIGIT_WIDTH = 4 // bits
)

// ID represents a 160-bit SHA-1 Identifier
type ID [BYTES]byte

// ZeroID is the default empty ID
var ZeroID ID

func init() {
	rand.Seed(time.Now().UnixNano())
}

// NewRandomID generates a random 160-bit ID
func NewRandomID() ID {
	var id ID
	rand.Read(id[:])
	return id
}

// Hash converts a string key to a Tapestry ID using SHA-1
func Hash(key string) ID {
	hash := sha1.Sum([]byte(key))
	return ID(hash)
}

// Parse converts a hex string to an ID
func Parse(s string) (ID, error) {
	var id ID
	b, err := hex.DecodeString(s)
	if err != nil {
		return id, err
	}
	if len(b) != BYTES {
		return id, fmt.Errorf("invalid ID length: expected %d bytes, got %d", BYTES, len(b))
	}
	copy(id[:], b)
	return id, nil
}

// String returns the Hex representation of the ID
func (id ID) String() string {
	return hex.EncodeToString(id[:])
}

// Bytes returns the byte slice
func (id ID) Bytes() []byte {
	return id[:]
}

// Equals checks if two IDs are identical
func (id ID) Equals(other ID) bool {
	return bytes.Equal(id[:], other[:])
}

// GetDigit returns the digit at a specific level (0 to 39)
// Level 0 is the most significant (left-most) hex digit.
func (id ID) GetDigit(level int) int {
	if level < 0 || level >= DIGITS {
		return -1
	}

	// Each byte contains 2 hex digits (nibbles).
	byteIdx := level / 2
	b := id[byteIdx]

	if level%2 == 0 {
		// Even level: High nibble (first 4 bits)
		return int((b >> 4) & 0x0F)
	}
	// Odd level: Low nibble (last 4 bits)
	return int(b & 0x0F)
}

// SharedPrefixLength returns the number of matching digits from the left (MSB)
func SharedPrefixLength(a, b ID) int {
	for i := 0; i < DIGITS; i++ {
		if a.GetDigit(i) != b.GetDigit(i) {
			return i
		}
	}
	return DIGITS
}

// Distance calculates the XOR distance between two IDs.
// This is used as a proxy for network distance when latency isn't available,
// or to determine "closeness" in the namespace.
func Distance(a, b ID) *big.Int {
	aInt := new(big.Int).SetBytes(a[:])
	bInt := new(big.Int).SetBytes(b[:])
	return new(big.Int).Xor(aInt, bInt)
}

// Closer returns true if candidate is "closer" to target than current is.
// Uses XOR metric.
func Closer(target, current, candidate ID) bool {
	dCurrent := Distance(target, current)
	dCandidate := Distance(target, candidate)
	// If dCandidate < dCurrent
	return dCandidate.Cmp(dCurrent) == -1
}

// SetDigit returns a COPY of the ID with the specific digit changed.
// Useful for calculating surrogate IDs.
func (id ID) SetDigit(level int, digit int) ID {
	if level < 0 || level >= DIGITS || digit < 0 || digit >= RADIX {
		return id
	}
	
	newID := id
	byteIdx := level / 2
	
	if level%2 == 0 {
		// Set high nibble: clear top 4 bits, OR in new digit shifted
		newID[byteIdx] = (newID[byteIdx] & 0x0F) | (byte(digit) << 4)
	} else {
		// Set low nibble: clear bottom 4 bits, OR in new digit
		newID[byteIdx] = (newID[byteIdx] & 0xF0) | byte(digit)
	}
	return newID
}