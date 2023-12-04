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

const WorkerTime = 10 * time.Second

// status of Coordinator
const (
	Map_Period    = "map"
	Reduce_Period = "reduce"
	All_Done      = "alldone"
)

type Coordinator struct {
	taskQueue []*Task
	index     int        // current task
	mutex     sync.Mutex // To lock shared data.
	c_status  string
	nReduce   int32
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GetTask(req *TaskRequest, resp *TaskResponse) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.c_status == All_Done {
		resp.Task_State = All_Task_Done
		return nil
	}
	// traverse through the taskQueue, and find a task to send it to worker
	Waiting := false
	n := len(c.taskQueue)
	for i := 0; i < n; i++ {
		t := c.taskQueue[c.index]
		c.index = (c.index + 1) % n

		// ask for a task successfully
		if t.Status == Status_Ready {
			t.Status = Status_Sent
			resp.Task = *t
			resp.Task_State = Initiate_Task
			go checkTask(c, t.TaskId, t.TaskType)
			return nil
		} else if t.Status == Status_Sent {
			// if some tasks is not finished yet
			Waiting = true
		}
	}

	// traverse the entire c.taskQueue to check if there are still some unfinished tasks,
	// tell worker to wait untill all the unfinished task to finish.
	if Waiting {
		resp.Task_State = Wait_Task
		return nil
	}

	// finish all map tasks during map peroid
	// or reduce tasks during reduce peroid
	switch c.c_status {
	case Map_Period:
		c.c_status = Reduce_Period
		loadReduceTasks(c)
		resp.Task_State = Wait_Task
		return nil
	case Reduce_Period:
		// end coordinator
		c.c_status = All_Done
		// end worker
		resp.Task_State = All_Task_Done
		return nil
	}
	return nil
}

// To check one task is finished or not by the worker wintin 10 seconds.
// if not finished, that means the worker might crashed.
func checkTask(c *Coordinator, taskId int32, taskType string) {
	time.Sleep(WorkerTime)
	c.mutex.Lock()
	defer c.mutex.Unlock()

	//to check if the coordinator is already at reduce period or not.
	//If so which means map-tasks all done then we will just ignore it.
	if c.taskQueue[taskId].TaskType != taskType {
		return
	}

	// Will reset the task.Status to Ready, so that it can be given out again.
	if c.taskQueue[taskId].Status == Status_Sent {
		c.taskQueue[taskId].Status = Status_Ready
	}
}

// Notification by worker about which task is finished
func (c *Coordinator) Notify(task_stat *NotifyTaskStatus, task_resp *NotifyResponse) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.taskQueue[task_stat.TaskId].Status = Status_Finish
	return nil
}

// load all map tasks to c.taskQueue
func loadMapTasks(c *Coordinator, filenames []string) {
	c.taskQueue = make([]*Task, 0)
	c.index = 0
	n := len(filenames)
	for i := 0; i < n; i++ {
		c.taskQueue = append(c.taskQueue, &Task{
			TaskId:   int32(i),
			TaskType: "map",
			Content:  filenames[i],
			Status:   Status_Ready,
		})
	}
}

// load all reduce tasks to c.taskQueue
func loadReduceTasks(c *Coordinator) {
	c.taskQueue = make([]*Task, 0)
	c.index = 0
	for i := 0; int32(i) < c.nReduce; i++ {
		c.taskQueue = append(c.taskQueue, &Task{
			TaskId:   int32(i),
			TaskType: "reduce",
			Content:  fmt.Sprint(c.nReduce),
			Status:   Status_Ready,
		})
	}
}

// for running server and worker on same machine
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

//for running server on different machine
/*func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":1234")
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}*/

// main/mrcoordinator.go calls Done() periodically to find out if the entire job has finished or not.
func (c *Coordinator) Done() bool {
	ret := false
	// Your code here.
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if c.c_status == All_Done { //if c.status == AllDone
		ret = true
	}
	return ret
}

// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		c_status: Map_Period,
		nReduce:  int32(nReduce),
	}
	// Your code here.
	loadMapTasks(&c, files)

	c.server()
	return &c
}
