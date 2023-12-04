package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"regexp"
	"sort"
	"strconv"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// ihash(key) % NReduce to choose the reduce
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function when worker is executed.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Send a request to the coordinator asking for a task via RPC.
	for {
		req := &TaskRequest{}
		resp := &TaskResponse{}
		if call("Coordinator.GetTask", req, resp) == true {
			switch resp.Task_State {
			case Wait_Task:
				time.Sleep(1 * time.Second)
				continue
			case All_Task_Done:
				break
			case Initiate_Task:
				switch resp.Task.TaskType {
				case "map":
					MapTask(resp.Task, mapf)
				case "reduce":
					ReduceTask(resp.Task, reducef)
				}
			}
		} else {
			break
		}
	}
}

// Map task section. Here key value pair is stored as {"key", "1"} in "mr-map-*" file
func MapTask(task Task, mapf func(string, string) []KeyValue) {
	//mapping section
	filename := task.Content
	file, _ := os.Open(filename)
	content, _ := io.ReadAll(file)
	defer file.Close()
	kva := mapf(filename, string(content))

	//create a temp file, and rename it after all written finished
	oname := fmt.Sprintf("mr-map-%v", task.TaskId)
	tmpfile, _ := os.CreateTemp(".", "temp-"+oname)

	enc := json.NewEncoder(tmpfile)
	for _, kv := range kva {
		if err := enc.Encode(&kv); err != nil {
			log.Fatalf("Map Task encoding failed. Error=%v\n", err)
		}
	}
	defer tmpfile.Close()
	os.Rename(tmpfile.Name(), oname)

	//notify the coordinator when task is done
	NotifyCoordinator(task.TaskId, task.TaskType)
}

// Reduce section. All files named "mr-map-*" is read and
// reduce for keys that satisfy ihash(key)%reduce == taskId,
// and store result in "mr-out-*"
func ReduceTask(task Task, reducef func(string, []string) string) {
	var kva []KeyValue
	files, _ := os.ReadDir(".")
	for _, file := range files {
		matched, _ := regexp.Match(`^mr-map-*`, []byte(file.Name()))
		if !matched {
			continue
		}
		filename := file.Name()
		file, _ := os.Open(filename)

		nReduce, _ := strconv.Atoi(task.Content)
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			if ihash(kv.Key)%nReduce == int(task.TaskId) {
				kva = append(kva, kv)
			}
		}
	}

	// do reduce just like in main/mrsequential.go
	sort.Sort(ByKey(kva))
	oname := fmt.Sprintf("mr-out-%v", task.TaskId)
	tmpfile, _ := os.CreateTemp(".", "temp-"+oname)
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		fmt.Fprintf(tmpfile, "%v %v\n", kva[i].Key, output)

		i = j
	}
	defer tmpfile.Close()
	os.Rename(tmpfile.Name(), oname)
	NotifyCoordinator(task.TaskId, task.TaskType)
}

// send Notify to coordinator, to tell coordinator that this task is finished
func NotifyCoordinator(taskId int32, taskType string) {
	task_stat := &NotifyTaskStatus{
		TaskId:   taskId,
		TaskType: taskType,
	}
	task_resp := &NotifyResponse{}
	call("Coordinator.Notify", task_stat, task_resp)
}

// send an RPC request to the coordinator, wait for the response.
func call(rpcname string, args interface{}, reply interface{}) bool {
	//c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
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
