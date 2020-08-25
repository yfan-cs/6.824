package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type AssignArgs struct {
// empty struct
}

type AssignReply struct {
	TaskType string // "Map" or "Reduce"
	RNum int // hashed number for reduce
	MNum int // map task number
	NReduce int // number of buckerts 
	Filename string // filename for map tasks
}


type FinishArgs struct {
	TaskType string // "Map" or "reduce"
	RNum int // hased number for reduce
	MNum int // map task number
	NewRNums []int // Reduce tasks after competion of the Map task
}

type FinishReply struct {
// empty struct
}


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
