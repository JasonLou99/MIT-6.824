package raft

import "log"

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.SetFlags(log.Ldate | log.Lmicroseconds)
		log.Printf(format, a...)
	}
	return
}

func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}
