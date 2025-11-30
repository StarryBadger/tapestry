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

const (
	BITS        = 160
	BYTES       = BITS / 8
	DIGITS      = 40 
	RADIX       = 16
	DIGIT_WIDTH = 4
)

type ID [BYTES]byte

var ZeroID ID

func init() {
	rand.Seed(time.Now().UnixNano())
}

func NewRandomID() ID {
	var id ID
	rand.Read(id[:])
	return id
}

func Hash(key string) ID {
	hash := sha1.Sum([]byte(key))
	return ID(hash)
}

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

func (id ID) String() string {
	return hex.EncodeToString(id[:])
}

func (id ID) Bytes() []byte {
	return id[:]
}

func (id ID) Equals(other ID) bool {
	return bytes.Equal(id[:], other[:])
}

func (id ID) GetDigit(level int) int {
	if level < 0 || level >= DIGITS {
		return -1
	}

	byteIdx := level / 2
	b := id[byteIdx]

	if level%2 == 0 {
		return int((b >> 4) & 0x0F)
	}
	return int(b & 0x0F)
}

func SharedPrefixLength(a, b ID) int {
	for i := 0; i < DIGITS; i++ {
		if a.GetDigit(i) != b.GetDigit(i) {
			return i
		}
	}
	return DIGITS
}

func Distance(a, b ID) *big.Int {
	aInt := new(big.Int).SetBytes(a[:])
	bInt := new(big.Int).SetBytes(b[:])
	return new(big.Int).Xor(aInt, bInt)
}

func Closer(target, current, candidate ID) bool {
	dCurrent := Distance(target, current)
	dCandidate := Distance(target, candidate)
	return dCandidate.Cmp(dCurrent) == -1
}

func (id ID) SetDigit(level int, digit int) ID {
	if level < 0 || level >= DIGITS || digit < 0 || digit >= RADIX {
		return id
	}
	
	newID := id
	byteIdx := level / 2
	
	if level%2 == 0 {
		newID[byteIdx] = (newID[byteIdx] & 0x0F) | (byte(digit) << 4)
	} else {
		newID[byteIdx] = (newID[byteIdx] & 0xF0) | byte(digit)
	}
	return newID
}