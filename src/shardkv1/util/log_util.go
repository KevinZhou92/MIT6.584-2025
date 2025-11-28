package util

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

// // Debugging
// const Debug = false

// func DPrintf(format string, a ...interface{}) {
// 	if Debug {
// 		log.Printf(format, a...)
// 	}
// }

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

type logTopic string

const (
	Error logTopic = "ERRO"
	Info  logTopic = "INFO"
	Warn  logTopic = "WARN"
	Fatal logTopic = "FATAL"
)

var debugStart time.Time
var debugVerbosity int

func init() {
	debugVerbosity = getVerbosity()
	debugStart = time.Now()

	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

func Debug(topic logTopic, format string, a ...interface{}) {
	if debugVerbosity < 1 {
		return
	}
	time := time.Since(debugStart).Microseconds()
	time /= 100
	prefix := fmt.Sprintf("%06d %v ", time, string(topic))
	format = prefix + format
	log.Printf(format, a...)
}
