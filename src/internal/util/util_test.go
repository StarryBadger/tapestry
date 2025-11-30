package util

import (
	"testing"
)

func TestGetDigit(t *testing.T) {
	var myID uint64 = 12345

	expectedDigits := []uint64{1, 2, 3, 0, 0, 0, 3}

	t.Logf("Testing ID %d...", myID)

	for i := 0; i < len(expectedDigits); i++ {
		actualDigit := GetDigit(myID, i)
		expectedDigit := expectedDigits[i]
		t.Logf("Digit %d: expected %d, got %d", i, expectedDigit, actualDigit)
		if actualDigit != expectedDigit {
			t.Errorf("Mismatch at digit %d: expected %d, but got %d", i, expectedDigit, actualDigit)
		}
	}
}