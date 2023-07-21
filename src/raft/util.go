package raft

import (
	"log"
	"sort"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func Median(arr []int) int {
	dup := make([]int, len(arr))
	copy(dup, arr)
	sort.Ints(dup)
	return dup[len(dup)/2]
}

func Max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func Min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
