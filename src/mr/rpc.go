package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

type TaskType int

const (
	MAP_TASK TaskType = iota
	REDUCE_TASK
	EXIT_TASK
	WAIT_TASK
)

type GetTaskRequest struct {
	Pid int
}

type GetTaskResponse struct {
	Task Task
}

type Task struct {
	TaskId    int
	FilePaths []string
	NReducer  int
	TaskType  TaskType
}

type TaskCompletionRequest struct {
	TaskId    int
	FilePaths map[int]string
	TaskType  TaskType
}

type TaskCompletionResponse struct {
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
