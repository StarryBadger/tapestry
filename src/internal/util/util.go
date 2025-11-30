package util

import "log"


func Assert(condition bool, msg string) {
	if !condition {
		log.Panic("Assertion failed: " + msg)
	}
}

func Debug(msg string) {
}