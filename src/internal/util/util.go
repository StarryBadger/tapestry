package util

import "log"

const RADIX = 4
const DIGITS = 32
const DIGIT_SHIFT = 2 
const DIGIT_MASK = RADIX - 1 

func GetDigit(h uint64, i int) uint64 {
	if i < 0 || i >= DIGITS {
		log.Panicf("Index out of range: %d", i)
	}
	return (h >> (i * DIGIT_SHIFT)) & DIGIT_MASK
}

func main() {
    var myID uint64 = 12345
    for i := 0; i < 5; i++ {
        log.Printf("Digit %d is %d\n", i, GetDigit(myID, i))
    }
}