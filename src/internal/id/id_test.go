package id

import (
	"math/big"
	"testing"
)

func TestSharedPrefixLength(t *testing.T) {
	// Case 1: Identical IDs
	id1 := Hash("test")
	if SharedPrefixLength(id1, id1) != DIGITS {
		t.Errorf("Identical IDs should have max prefix length")
	}

	// Case 2: Known difference
	// We'll manually construct IDs to test specific digit matches
	var a, b ID
	// Set first digit to 1 for both
	a = a.SetDigit(0, 1)
	b = b.SetDigit(0, 1)
	// Set second digit to 5 vs 6
	a = a.SetDigit(1, 5)
	b = b.SetDigit(1, 6)

	length := SharedPrefixLength(a, b)
	if length != 1 {
		t.Errorf("Expected prefix length 1, got %d", length)
	}
}

func TestSetDigit(t *testing.T) {
	var id ID
	// Set 3rd digit (index 2) to 'F' (15)
	id = id.SetDigit(2, 15)
	
	val := id.GetDigit(2)
	if val != 15 {
		t.Errorf("Expected digit 15, got %d", val)
	}

	// Ensure other digits are 0
	if id.GetDigit(0) != 0 || id.GetDigit(3) != 0 {
		t.Errorf("SetDigit affected other digits")
	}
}

func TestCloser(t *testing.T) {
	target := Hash("target")
	
	// Create an ID very close to target (only last digit different)
	closeID := target
	originalLast := target.GetDigit(DIGITS - 1)
	closeID = closeID.SetDigit(DIGITS-1, (originalLast+1)%RADIX)

	// Create an ID far away (first digit different)
	farID := target
	originalFirst := target.GetDigit(0)
	farID = farID.SetDigit(0, (originalFirst+1)%RADIX)

	if !Closer(target, farID, closeID) {
		t.Errorf("closeID should be closer to target than farID")
	}
}

func TestDistance(t *testing.T) {
	var a, b ID
	// Distance between 0 and 1 should be 1
	b[BYTES-1] = 1 
	
	dist := Distance(a, b)
	if dist.Cmp(big.NewInt(1)) != 0 {
		t.Errorf("Expected distance 1, got %v", dist)
	}
}