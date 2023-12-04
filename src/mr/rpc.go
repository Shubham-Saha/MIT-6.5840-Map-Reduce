package mr

import (
	"os"
	"strconv"
)

// Add your RPC definitions here.

// TaskResponse
const (
	Initiate_Task = "initiate"
	Wait_Task     = "wait"
	All_Task_Done = "done"
)

// status of task
const (
	Status_Ready  = "ready"
	Status_Sent   = "sent"
	Status_Finish = "finish"
)

// Task structure
type Task struct {
	TaskId   int32
	TaskType string
	Content  string
	Status   string
}

type TaskRequest struct {
}

type TaskResponse struct {
	Task_State string
	Task       Task
}

type NotifyTaskStatus struct {
	TaskId   int32
	TaskType string
}

type NotifyResponse struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
