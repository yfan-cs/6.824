package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "io/ioutil"
import "sort"
import "encoding/json"
import "path/filepath"

type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


func workerMap(mapf func(string, string) []KeyValue,
	reply *AssignReply) {
	filename := reply.Filename
	nReduce := reply.NReduce
	mNum := reply.MNum

	// map from reduce task number to corresponding keyvalue pairs
	intermediate := make(map[int][]KeyValue)
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	for _, kv := range kva {
	    rNum := ihash(kv.Key) % nReduce
	    intermediate[rNum] = append(intermediate[rNum], kv) 
	}
	newRTasks := make([]int, 0)

	for rNum := range intermediate {
	    newRTasks = append(newRTasks, rNum)
	    sort.Sort(ByKey(intermediate[rNum]))
	    oname := fmt.Sprintf("mr-%d-%d", mNum, rNum)
	    ofile, _ := os.Create(oname)
	    enc := json.NewEncoder(ofile)
	    for _, kv := range intermediate[rNum] {
	        //err := enc.Encode(&kv)
		enc.Encode(&kv)
	    }
	}
	
	// notify the master of the completion
	finishArgs := FinishArgs {
		TaskType: "Map",
		MNum: mNum,
		NewRNums: newRTasks,
	} 
	CallFinishTask(&finishArgs)	
}


func workerReduce(reducef func(string, []string) string,
	reply *AssignReply) {
	rNum := reply.RNum // reduce task number
	kva := []KeyValue{}
	pattern := fmt.Sprintf("*-%d", rNum)
	filenames, _ := filepath.Glob(pattern)

	for _, filename := range filenames {
	    file, _ := os.Open(filename)
	    dec := json.NewDecoder(file)
	    for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
		    break
		}
		kva = append(kva, kv)
	    }
	    file.Close()
	}
	oname := fmt.Sprintf("mr-out-%d", rNum)
	ofile, _ := os.Create(oname)
	sort.Sort(ByKey(kva))
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

                // this is the correct format for each line of Reduce output.
                fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

                i = j
        }

        ofile.Close()

	
	// notify the master of the completion
	finishArgs := FinishArgs {
		TaskType: "Reduce",
		RNum: rNum,
	} 
	CallFinishTask(&finishArgs)	
}
	
//
// main/mrworker.go calls this function.
// put intermediate Map output in files in the current diretory, where
// worker can read as input to Reduce tasks.
// mr-X-Y: X is the Map task numbre, Y is the reduce task number.
// put X'th reduce task in the file mr-out-X
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		reply, succeeded := CallAssignTask()
		if !succeeded {
		    return
		}

		// first acquire a task from master
		// if it is a map task:
		//
		if reply.TaskType == "Map" {
		    workerMap(mapf, reply)
		} else {
		    workerReduce(reducef, reply)
		}
	
	}

}

//
// Call Master to assign a task to worker
// the RPC argument and reply types are defined in rpc.go.
//
func CallAssignTask() (*AssignReply, bool){

	// declare an argument structure.
	args := AssignArgs{}

	// fill in the argument(s).
	// args.X = 99

	// declare a reply structure.
	reply := AssignReply{}

	// send the RPC request, wait for the reply.
	succeeded := call("Master.AssignTask", &args, &reply)
	
	return &reply, succeeded
}

//
// Call Master to notify the finish of a task
// the RPC argument and reply types are defined in rpc.go.
//
func CallFinishTask(args *FinishArgs) {

	// declare a reply structure.
	reply := FinishReply{}

	call("Master.FinishTask", args, &reply)
}


//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
