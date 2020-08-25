package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "time" // for time out of slow workers


type Master struct {
	mapTasks []mapTask
	reduceTasks map[int]*reduceTask
	nReduce int // number of reduce buckets
        mapDone bool // whether all the map tasks are done
	mux sync.Mutex
}

const (
	IDLE = 0
	PROGRESS = 1
	FINISHED = 2
	TIMEOUT = 10 // time out in seconds
)

// separate Map and Reduce tasks because
// Reduce task cannot start until all Map tasks finish.
type mapTask struct {
	state int // idle, in-progress, completed
	filename string // filename for Map tasks
	startedTime time.Time // time when worker started the task
}

type reduceTask struct {
	state int // idle, in-progress, completed
	taskID int // task number for reduce tasks
	startedTime time.Time // time when worker started the task
}

// Your code here -- RPC handlers for the worker to call.

// assign tasks to workers when workers call this function
func (m *Master) AssignTask(args *AssignArgs, reply *AssignReply) error {

	for {
	    m.mux.Lock()
	    if !m.mapDone {
		    for i := range m.mapTasks {
			if m.mapTasks[i].state == IDLE {
			    reply.TaskType = "Map"
			    reply.MNum = i
			    reply.Filename = m.mapTasks[i].filename
			    reply.NReduce = m.nReduce
			    m.mapTasks[i].state = PROGRESS
			    m.mapTasks[i].startedTime = time.Now()
			    m.mux.Unlock()
			    return nil
			}
		    }  
	    } else {
		    for k := range m.reduceTasks {
			if m.reduceTasks[k].state == IDLE {
			    reply.TaskType = "Reduce"
			    reply.RNum = m.reduceTasks[k].taskID
			    m.reduceTasks[k].state = PROGRESS
			    m.reduceTasks[k].startedTime = time.Now()
			    m.mux.Unlock()
			    return nil
			}
		    }
	    }
	    m.mux.Unlock()

	    time.Sleep(time.Second)
	}
}

// update task status when workers finish execution and call this function
func (m *Master) FinishTask(args *FinishArgs, reply *FinishReply) error {
	m.mux.Lock()
	defer m.mux.Unlock()
	if args.TaskType == "Map" {
	    mNum := args.MNum
	    for i := range m.mapTasks {
	        if i == mNum {
	            m.mapTasks[i].state = FINISHED
		    // add new Reduce tasks
		    for _, newR := range args.NewRNums {
		        if _, ok := m.reduceTasks[newR]; !ok {
			    m.reduceTasks[newR] = &reduceTask{state: IDLE, taskID: newR} 
			} 
		    }  
		    return nil
		}
	    }
	} else {
	    rNum := args.RNum
	    m.reduceTasks[rNum].state = FINISHED
	}
	
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// thread that checks 'died' worker periodically
func (m *Master) checkDiedWorker() {
	for {
	    time.Sleep(time.Second)
	    m.mux.Lock()
	    defer m.mux.Unlock()
	    curTime := time.Now()
	    if !m.mapDone {
		    for i := range m.mapTasks {
			if m.mapTasks[i].state == PROGRESS && curTime.Sub(m.mapTasks[i].startedTime).Seconds() > TIMEOUT  {
			    m.mapTasks[i].state = IDLE
			}
		    }  
	    } else {
		    for k := range m.reduceTasks {
			if m.reduceTasks[k].state == PROGRESS && curTime.Sub(m.reduceTasks[k].startedTime).Seconds() > TIMEOUT {
			    m.reduceTasks[k].state = IDLE
			}
		    }
	    }
	    m.mux.Unlock()
	}
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
	go m.checkDiedWorker()
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	// see if Map tasks are done
	m.mux.Lock()
	defer m.mux.Unlock()
	if !m.mapDone {
		for _, task := range m.mapTasks {
		    if task.state != FINISHED {
			return false
		    }
		}
		m.mapDone = true
	} else {
		for _, task := range m.reduceTasks {
		    if task.state != FINISHED {
			return false
		    }
		}
		return true
	}
	return false
}

//
// create a Master.
//
func MakeMaster(files []string, nReduce int) *Master {
	mapTasks := make([]mapTask,0)
	for _, filename := range files {
	    mapTasks = append(mapTasks, mapTask {state: IDLE, filename: filename, startedTime: time.Now()}) 
	}
	reduceTasks := make(map[int]*reduceTask)
	m := Master{mapTasks: mapTasks, reduceTasks: reduceTasks, nReduce: nReduce, mapDone: false}
	m.server()
	return &m
}
