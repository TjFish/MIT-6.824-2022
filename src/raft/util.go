package raft

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

func isTimeOut(lastTime time.Time, timeOut time.Duration) bool {
	return time.Now().Sub(lastTime) > timeOut
}

type Ticker struct {
	elapsedTime time.Duration
	lastTime    time.Time
}

func (t *Ticker) elapsed() {
	t.elapsedTime += time.Now().Sub(t.lastTime)
	t.lastTime = time.Now()
}

func (t *Ticker) isTimeOut(timeOut time.Duration) bool {
	return t.elapsedTime > timeOut
}

func (t *Ticker) reset() {
	t.elapsedTime = 0
	t.lastTime = time.Now()
}

func Min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

// Debugging
const debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if debug {
		log.Printf(format, a...)
	}
	return
}

type logTopic string

const (
	dClient  logTopic = "CLNT"
	dCommit  logTopic = "CMIT"
	dDrop    logTopic = "DROP"
	dError   logTopic = "ERRO"
	dInfo    logTopic = "INFO"
	dLeader  logTopic = "LEAD"
	dLog     logTopic = "LOG1"
	dLog2    logTopic = "LOG2"
	dPersist logTopic = "PERS"
	dSnap    logTopic = "SNAP"
	dTerm    logTopic = "TERM"
	dTest    logTopic = "TEST"
	dTimer   logTopic = "TIMR"
	dTrace   logTopic = "TRCE"
	dVote    logTopic = "VOTE"
	dWarn    logTopic = "WARN"
)

// Retrieve the verbosity level from an environment variable
func getVerbosity() int {
	v := os.Getenv("VERBOSE")
	level := 0
	if v != "" {
		var err error
		level, err = strconv.Atoi(v)
		if err != nil {
			log.Fatalf("Invalid verbosity %v", v)
		}
	}
	return level
}

var debugStart time.Time
var debugVerbosity int

func init() {
	debugVerbosity = getVerbosity()
	debugStart = time.Now()

	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

func Debug(topic logTopic, format string, a ...interface{}) {
	if debug {
		time := time.Since(debugStart).Microseconds()
		time /= 100
		prefix := fmt.Sprintf("%06d %v ", time, string(topic))
		format = prefix + format
		log.Printf(format, a...)
	}
}
