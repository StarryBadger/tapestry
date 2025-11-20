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

func DigitToChar(d uint64) rune {
	if d >= RADIX {
		log.Panicf("digit out of range: %d", d)
	}
	return rune('0' + d)
}

func HashToString(h uint64) string {
	if h == 0 {
		return "0" 
	}
	result := make([]rune, DIGITS)
	for i := 0; i < DIGITS; i++ {
		digit := GetDigit(h, DIGITS-1-i)
		result[i] = DigitToChar(digit)
	}
	return string(result)
}

func StringToHash(s string) uint64 {
	var h uint64 = 0
	for _, char := range s {
		var digit uint64
		if '0' <= char && char <= '3' {
			digit = uint64(char - '0')
		} else {
			log.Panicf("Invalid character in hash string: %c", char)
		}
		h = (h << DIGIT_SHIFT) | digit
	}
	return h
}