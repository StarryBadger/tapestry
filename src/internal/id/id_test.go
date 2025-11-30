package id

import (
	"math/big"
	"testing"
)

func TestSharedPrefixLength(t *testing.T) {
	id1 := Hash("test")
	if SharedPrefixLength(id1, id1) != DIGITS {
		t.Errorf("Identical IDs should have max prefix length")
	}

	var a, b ID
	a = a.SetDigit(0, 1)
	b = b.SetDigit(0, 1)
	a = a.SetDigit(1, 5)
	b = b.SetDigit(1, 6)

	length := SharedPrefixLength(a, b)
	if length != 1 {
		t.Errorf("Expected prefix length 1, got %d", length)
	}
}

func TestSetDigit(t *testing.T) {
	var id ID
	id = id.SetDigit(2, 15)
	
	val := id.GetDigit(2)
	if val != 15 {
		t.Errorf("Expected digit 15, got %d", val)
	}

	if id.GetDigit(0) != 0 || id.GetDigit(3) != 0 {
		t.Errorf("SetDigit affected other digits")
	}
}

func TestCloser(t *testing.T) {
	target := Hash("target")
	
	closeID := target
	originalLast := target.GetDigit(DIGITS - 1)
	closeID = closeID.SetDigit(DIGITS-1, (originalLast+1)%RADIX)

	farID := target
	originalFirst := target.GetDigit(0)
	farID = farID.SetDigit(0, (originalFirst+1)%RADIX)

	if !Closer(target, farID, closeID) {
		t.Errorf("closeID should be closer to target than farID")
	}
}

func TestDistance(t *testing.T) {
	var a, b ID
	b[BYTES-1] = 1 
	
	dist := Distance(a, b)
	if dist.Cmp(big.NewInt(1)) != 0 {
		t.Errorf("Expected distance 1, got %v", dist)
	}
}