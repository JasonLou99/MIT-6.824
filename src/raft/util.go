package raft

import "log"

// Debugging
const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.SetFlags(log.Ldate | log.Lmicroseconds)
		log.Printf(format, a...)
	}
	return
}

func min(x int, y int) int {
	if x > y {
		return y
	} else {
		return x
	}
}
