package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

var PID int = os.Getpid()

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	shouldExit := false
	for !shouldExit {
		taskId, filePaths, nReduce, taskType := callGetTask()
		fmt.Printf("= Get a task for taskId: %d\n", taskId)
		// Didn't get any task, sleep for 1 sec and try to get a task
		switch taskType {
		case MAP_TASK:
			executeMapTask(taskId, filePaths, nReduce, mapf)
		case REDUCE_TASK:
			executeReduceTask(taskId, filePaths, reducef)
		case WAIT_TASK:
			time.Sleep(2 * time.Second)
			continue
		case EXIT_TASK:
			fmt.Println("= Task Done. Worker Exiting")
			shouldExit = true
		}
	}
}

func executeMapTask(taskId int, filePaths []string, nReduce int, mapf func(string, string) []KeyValue) {
	fmt.Printf("= Map task with task id %d and filePath %s\n", taskId, filePaths)
	filePath := filePaths[0]
	output := make(map[int]string)
	// Handle map job
	intermediate := []KeyValue{}

	// Open file and start processing
	file, err := os.Open(filePath)
	if err != nil {
		log.Fatalf("cannot open %v", filePath)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filePath)
	}
	file.Close()
	kva := mapf(filePath, string(content))
	intermediate = append(intermediate, kva...)

	partitions := make(map[int][]KeyValue)
	for idx := range intermediate {
		reduceBucketId := ihash(intermediate[idx].Key) % nReduce
		partitions[reduceBucketId] = append(partitions[reduceBucketId], intermediate[idx])
	}

	for idx, kva := range partitions {
		oname := fmt.Sprintf("mr-%d-%d", taskId, idx)
		ofile, _ := os.Create(oname)
		enc := json.NewEncoder(ofile)
		for _, kv := range kva {
			err := enc.Encode(&kv)
			if err != nil {
				fmt.Println("= Error saving output")
			}
		}

		ofile.Close()
		output[idx] = oname
	}
	callFinishTask(taskId, output, MAP_TASK)
	fmt.Printf("= Finished map task for taskId: %d\n", taskId)
}

func executeReduceTask(taskId int, filePaths []string, reducef func(string, []string) string) {
	// Reduce Task
	intermediate := []KeyValue{}
	for _, filename := range filePaths {
		fmt.Println("= Reduce file path: " + filename)
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				fmt.Printf("= Cannot reduce file %s due to %s\n", filename, err)
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}

	sort.Sort(ByKey(intermediate))

	// Create a tmp file to store reduce output
	// avoid partial results being seen before full completion
	tmpFile, _ := getTmpFile("", "tmp")
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(tmpFile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	tmpFile.Close()

	oname := fmt.Sprintf("mr-out-%d", taskId)
	os.Rename(tmpFile.Name(), oname)
	callFinishTask(taskId, nil, REDUCE_TASK)
}

func callGetTask() (int, []string, int, TaskType) {
	// declare an argument structure.
	args := GetTaskRequest{
		Pid: PID,
	}

	// declare a reply structure.
	reply := GetTaskResponse{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.GetTask", &args, &reply)
	if ok {
		return reply.Task.TaskId, reply.Task.FilePaths, reply.Task.NReducer, reply.Task.TaskType
	} else {
		fmt.Printf("call failed!\n")
		return -1, []string{}, -1, EXIT_TASK
	}
}

func callFinishTask(id int, output map[int]string, taskType TaskType) {
	// declare an argument structure.
	request := TaskCompletionRequest{
		TaskId:    id,
		FilePaths: output,
		TaskType:  taskType,
	}

	// declare a reply structure.
	reply := TaskCompletionResponse{}

	ok := call("Coordinator.FinishTask", &request, &reply)
	if ok {
		fmt.Printf("= Task.Output %v\n", output)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

func getTmpFile(dir string, pattern string) (*os.File, error) {
	file, err := os.CreateTemp(dir, pattern)
	if err != nil {
		fmt.Println("= can't create tmp file")
		panic(err)
	}

	return file, nil
}
