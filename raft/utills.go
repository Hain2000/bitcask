package raft

import (
	"go.uber.org/zap"
	"math/rand"
	"time"
)

func RandIntRange(min, max int) int {
	s1 := rand.NewSource(time.Now().UnixNano())
	r1 := rand.New(s1)
	return r1.Intn(max-min) + min
}

func MakeAnRandomElectionTimeout(base int) int {
	return RandIntRange(base, base*2)
}

func PrintDebugLog(msg string) {
	log, err := zap.NewDevelopment()
	if err != nil {
		panic(err)
	}
	log.Sugar().Debugf("%s %s \n", time.Now().Format("2006-01-02 15:04:05"), msg)
}

func Min(x, y int) int {
	if x > y {
		return y
	}
	return x
}

func Max(x, y int) int {
	if x < y {
		return y
	}
	return x
}
