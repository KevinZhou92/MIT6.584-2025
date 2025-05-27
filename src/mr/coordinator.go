package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Status int

const (
	NOT_STARTED Status = iota
	STARTED
	COMPLETED
)

type State struct {
	status    Status
	startTime int
}

var statusToString = map[Status]string{
	NOT_STARTED: "Not Started",
	STARTED:     "Started",
	COMPLETED:   "Completed",
}

var taskTypeToString = map[TaskType]string{
	MAP_TASK:    "Map Task",
	REDUCE_TASK: "Reduce Task",
}

type Coordinator struct {
	// Your definitions here.
	files      []string
	mTasks     map[int][]string
	mTaskState map[int]*State
	rTasks     map[int][]string
	rTaskState map[int]*State
	nReducer   int
	lock       sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) GetTask(request *GetTaskRequest, reply *GetTaskResponse) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	fmt.Printf("= Request request from worker %d to get a task\n", request.Pid)
	// Default wait task
	waitTask := c.assignTaskForTaskType(WAIT_TASK)
	reply.Task = *waitTask

	if mapTask := c.assignTaskForTaskType(MAP_TASK); mapTask != nil {
		fmt.Printf("= Get an available map task with task id %d\n", mapTask.TaskId)
		reply.Task = *mapTask
		return nil
	}

	// Default response if there is not output
	if !c.areAllTasksDone(MAP_TASK) {
		return nil
	}

	// Try to get a reduce task
	if reduceTask := c.assignTaskForTaskType(REDUCE_TASK); reduceTask != nil {
		reply.Task = *reduceTask
		return nil
	}

	if c.areAllTasksDone(MAP_TASK, REDUCE_TASK) {
		// Default response if there is not output
		exitTask := c.assignTaskForTaskType(EXIT_TASK)
		reply.Task = *exitTask
		return nil
	}

	return nil
}

func (c *Coordinator) areAllTasksDone(taskTypes ...TaskType) bool {
	allTasksDone := true
	for _, taskType := range taskTypes {
		switch taskType {
		case MAP_TASK:
			for taskId := range c.mTaskState {
				if c.mTaskState[taskId].status != COMPLETED {
					allTasksDone = false
					break
				}
			}
		case REDUCE_TASK:
			for taskId := range c.rTaskState {
				if c.rTaskState[taskId].status != COMPLETED {
					allTasksDone = false
					break
				}
			}
		}
	}

	fmt.Printf("= AllTasksDone: %t.\n", allTasksDone)

	return allTasksDone
}

func (c *Coordinator) assignTaskForTaskType(taskType TaskType) *Task {
	if taskType == MAP_TASK {
		return c.assignTask(c.mTasks, c.mTaskState, MAP_TASK)
	} else if taskType == REDUCE_TASK {
		return c.assignTask(c.rTasks, c.rTaskState, REDUCE_TASK)
	} else {
		return &Task{
			TaskId:    -1,
			FilePaths: []string{},
			NReducer:  -1,
			TaskType:  taskType,
		}
	}
}

func (c *Coordinator) assignTask(tasks map[int][]string, taskState map[int]*State, taskType TaskType) *Task {
	// Try to get mapper tasks
	for taskId := range tasks {
		taskState := taskState[taskId]
		if taskState.status == COMPLETED || (taskState.status == STARTED && taskState.startTime+int(10*time.Second.Seconds()) > int(time.Now().Unix())) {
			fmt.Printf("= %s TaskId %d is %s. Skip...\n", taskTypeToString[taskType], taskId, statusToString[taskState.status])
			continue
		}
		fmt.Printf("= %s TaskId %d with filePath %s will be assigned.\n", taskTypeToString[taskType], taskId, tasks[taskId])
		taskState.status = STARTED
		taskState.startTime = int(time.Now().Unix())

		return &Task{
			TaskId:    taskId,
			TaskType:  taskType,
			NReducer:  c.nReducer,
			FilePaths: tasks[taskId],
		}
	}

	return nil
}

func (c *Coordinator) FinishTask(request *TaskCompletionRequest, reply *TaskCompletionResponse) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	taskId, filePaths, taskType := request.TaskId, request.FilePaths, request.TaskType
	switch taskType {
	case MAP_TASK:
		c.mTaskState[taskId].status = COMPLETED
		fmt.Printf("= Map TaskId %d is %s. Skip...\n", taskId, statusToString[c.mTaskState[taskId].status])
		for reduceId, filePath := range filePaths {
			fmt.Printf("= Map job resulting file paths: %s for reducer id %d\n", filePath, taskId)
			c.rTasks[reduceId] = append(c.rTasks[reduceId], filePath)
		}
	case REDUCE_TASK:
		c.rTaskState[taskId].status = COMPLETED
		fmt.Printf("= Reduce TaskId %d is %s. Skip...\n", taskId, statusToString[c.rTaskState[taskId].status])
	default:
		fmt.Printf("Unknown Task Type: %d\n", taskType)
	}

	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)

}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.lock.Lock()
	defer c.lock.Unlock()
	ret := c.areAllTasksDone(MAP_TASK, REDUCE_TASK)

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// Map tasks, id -> file path
	mTasks := make(map[int][]string)
	mTaskState := make(map[int]*State)
	rTasks := make(map[int][]string)
	rTaskState := make(map[int]*State)
	// Loop through the files and set each file's value to false
	for idx, f := range files {
		mTasks[idx] = []string{f}
		mTaskState[idx] = &State{
			status:    NOT_STARTED,
			startTime: 0,
		}
	}
	fmt.Printf("= Created %d Map tasks\n", len(mTasks))

	// Build reduce task bucket
	for idx := 0; idx < nReduce; idx++ {
		rTasks[idx] = make([]string, 0)
		rTaskState[idx] = &State{
			status:    NOT_STARTED,
			startTime: 0,
		}
	}
	fmt.Printf("= Created %d Reduce tasks\n", len(rTasks))

	c := Coordinator{
		files:      files,
		mTasks:     mTasks,
		mTaskState: mTaskState,
		rTasks:     rTasks,
		rTaskState: rTaskState,
		nReducer:   nReduce,
	}

	c.server()

	return &c
}
