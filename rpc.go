package mr

//
// RPC definitions.
//

import (
	"os"
	"strconv"
)


type GetTaskArgs struct {
}

type GetTaskReply struct {
	T       Task
	NReduce int
	NMap    int
	Wait    bool
}

type ReportTaskArgs struct {
	Success     bool
	Task_Number int
	Task_Type   string
}

type ReportTaskReply struct {
	Received bool
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
