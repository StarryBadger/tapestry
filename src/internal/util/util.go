// package util

// import "log"

// // const RADIX = 4
// // const DIGITS = 32
// // const DIGIT_SHIFT = 2 
// // const DIGIT_MASK = RADIX - 1 

// func GetDigit(h uint64, i int) uint64 {
// 	if i < 0 || i >= DIGITS {
// 		log.Panicf("Index out of range: %d", i)
// 	}
// 	return (h >> (i * DIGIT_SHIFT)) & DIGIT_MASK
// }

// func DigitToChar(d uint64) rune {
// 	if d >= RADIX {
// 		log.Panicf("digit out of range: %d", d)
// 	}
// 	return rune('0' + d)
// }

// func HashToString(h uint64) string {
// 	if h == 0 {
// 		return "0" 
// 	}
// 	result := make([]rune, DIGITS)
// 	for i := 0; i < DIGITS; i++ {
// 		digit := GetDigit(h, DIGITS-1-i)
// 		result[i] = DigitToChar(digit)
// 	}
// 	return string(result)
// }

// func StringToHash(s string) uint64 {
// 	var h uint64 = 0
// 	for _, char := range s {
// 		var digit uint64
// 		if '0' <= char && char <= '3' {
// 			digit = uint64(char - '0')
// 		} else {
// 			log.Panicf("Invalid character in hash string: %c", char)
// 		}
// 		h = (h << DIGIT_SHIFT) | digit
// 	}
// 	return h
// }

// func Assert(condition bool, msg string) {
// 	if !condition {
// 		log.Panic("Assertion failed: " + msg)
// 	}
// }

// // convert a 2D slice of ints into a 1D slice of int32s for proto messages.
// func FlattenMatrix(matrix [][]int) []int32 {
// 	var flat []int32
// 	for _, row := range matrix {
// 		for _, val := range row {
// 			flat = append(flat, int32(val))
// 		}
// 	}
// 	return flat
// }

// // convert a 1D slice of int32s back into a 2D slice of ints
// func UnflattenMatrix(flat []int32, rows, cols int) [][]int {
// 	Assert(len(flat) == rows*cols, "data length does not match rows * cols")

// 	matrix := make([][]int, rows)
// 	for i := 0; i < rows; i++ {
// 		start := i * cols
// 		row := make([]int, cols)
// 		for j := 0; j < cols; j++ {
// 			row[j] = int(flat[start+j])
// 		}
// 		matrix[i] = row
// 	}
// 	return matrix
// }

// func CommonPrefixLen(a, b uint64) int {
// 	len := 0
// 	for i := 0; i < DIGITS; i++ {
// 		if GetDigit(a, i) == GetDigit(b, i) {
// 			len++
// 		} else {
// 			break
// 		}
// 	}
// 	return len
// }

package util

import "log"

// Generic helpers can stay here.
// ID-specific constants (RADIX, DIGITS) have moved to internal/id

func Assert(condition bool, msg string) {
	if !condition {
		log.Panic("Assertion failed: " + msg)
	}
}

// Helper to print debug info if needed
func Debug(msg string) {
	// log.Println(msg)
}