package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
)
import "log"
import "net/rpc"
import "hash/fnv"

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

// Use ihash(key) % NReduce to choose the rIdx for each keyValue emitted by Map.
func keyReduceIndex(key string, nReduce int) int {
	return ihash(key) % nReduce
}

//const pathPrefix = "./"
//
//func makePgFileName(name *string) string {
//	return "pg-" + *name + ".text"
//}

// makeIntermediateFromFile gets intermediate given fileName and mapf
func makeIntermediateFromFile(fileName string, mapf func(string, string) []KeyValue) []KeyValue {
	path := fileName
	file, err := os.Open(path)
	if err != nil {
		log.Fatalf("cannot open %v", path)
	}

	// read the fileContent
	fileContent, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read file %v", fileName)
	}
	file.Close()

	// generate kv array
	kva := mapf(fileName, string(fileContent))
	return kva
}

type AWorker struct {
	mapf    func(string, string) []KeyValue
	reducef func(string, []string) string

	// false on map
	// true on reduce
	mapOrReduce bool

	// must exit if true
	allDone bool

	workerId int
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	// Initialize a worker
	worker := AWorker{}
	worker.mapf = mapf
	worker.reducef = reducef
	worker.mapOrReduce = false
	worker.allDone = false
	worker.workerId = -1

	worker.logPrintf("initialized!\n")

	// main process
	for !worker.allDone {
		// wait for a random little while
		// rand.Seed(time.Now().UnixNano())
		// n := rand.Intn(10)
		// log.Printf("wait for %vms\n", n*100)
		// time.Sleep(1000 * time.Millisecond)
		worker.process()
	}

}

func (worker *AWorker) logPrintf(format string, vars ...interface{}) {
	tmp := "worker %d: " + format
	log.Printf(tmp, worker.workerId, vars)
}

// askMapTask asks for map task and gets reply from the coordinator, including fileName,
// fileId, nReduce, workerId, allDone, for mapf
func (worker *AWorker) askMapTask() *MapTaskReply {

	// Initialize the args and reply container.
	args := MapTaskArgs{}
	args.WorkerId = worker.workerId
	reply := MapTaskReply{}

	worker.logPrintf("requesting for map task...\n")
	// Call coordinator
	call("Coordinator.GiveMapTask", &args, reply)
	// worker.logPrintf("request for map task received...\n")

	// obtain a worker id no matter what
	// Get workerId from the coordinator
	worker.workerId = reply.WorkerId

	if reply.FileId == -1 {
		// refused to give a task
		// notify the caller
		if reply.AllDone {
			worker.logPrintf("no more map tasks, switch to reduce mode\n")
			return nil
		} else {
			// worker.logPrintf("there is no task available for now. there will be more...")
			return &reply
		}
	}

	worker.logPrintf("got map task on file %v %v\n", reply.FileId, reply.FileName)

	return &reply
}

// writeToFiles writes intermediate into files with pattern "mr-fileId-rIdx"
func (worker *AWorker) writeToFiles(fileId int, nReduce int, intermediate []KeyValue) {
	// kvas[rIdx]: assign each pair of KeyValue to their rIdx.
	kvas := make([][]KeyValue, nReduce)
	for i := 0; i < nReduce; i++ {
		kvas[i] = make([]KeyValue, 0)
	}
	for _, kv := range intermediate {
		rIdx := keyReduceIndex(kv.Key, nReduce)
		kvas[rIdx] = append(kvas[rIdx], kv)
	}

	// Create TempFile for each reduce task rIdx
	for i := 0; i < nReduce; i++ {
		tempFile, err := os.CreateTemp(".", "mrtemp")
		if err != nil {
			log.Fatal(err)
		}

		// Create new Encoder
		enc := json.NewEncoder(tempFile)
		for _, kv := range kvas[i] {
			// encode each pair of keyValue. Iterate the whole row.
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatal(err)
			}
		}
		// Create name for intermediate files. mr-fileId-rIdx
		outName := fmt.Sprintf("mr-%v-%v", fileId, i)
		err = os.Rename(tempFile.Name(), outName)
		if err != nil {
			worker.logPrintf("rename tempFile failed for %v\n", outName)
		}
	}
}

func (worker *AWorker) joinMapTask(fileId int) {
	args := MapTaskJoinArgs{}
	args.FileId = fileId
	args.WorkerId = worker.workerId

	reply := MapTaskJoinReply{}
	call("Coordinator.JoinMapTask", &args, &reply)

	if reply.Accept {
		worker.logPrintf("accepted\n")
	} else {
		worker.logPrintf("not accepted\n")
	}
}

func (worker *AWorker) executeMap(reply *MapTaskReply) {
	intermediate := makeIntermediateFromFile(reply.FileName, worker.mapf)
	worker.logPrintf("writing map results to file\n")
	worker.writeToFiles(reply.FileId, reply.NReduce, intermediate)
	worker.joinMapTask(reply.FileId)
}

func (worker *AWorker) askReduceTask() *ReduceTaskReply {
	args := ReduceTaskArgs{}
	// Remain the same workerId.
	args.WorkerId = worker.workerId
	reply := ReduceTaskReply{}

	worker.logPrintf("requesting for reduce task...\n")
	// Call coordinator
	call("Coordinator.GiveReduceTask", &args, reply)

	if reply.RIndex == -1 {
		if reply.AllDone {
			worker.logPrintf("no more reduce tasks, try to terminate worker\n")
			return nil
		} else {
			// worker.logPrintf("there is no task available for now. there will be more just a moment...\n")
			return &reply
		}
	}
	worker.logPrintf("got reduce task on %vth cluster", reply.RIndex)
	return &reply
}

// for sorting by key
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// reduceKVSlice
func reduceKVSlice(intermediate []KeyValue, reducef func(string, []string) string, ofile io.Writer) {
	sort.Sort(ByKey(intermediate))
	for i := 0; i < len(intermediate); {
		j := i + 1
		for j < len(intermediate) && (intermediate)[j].Key == (intermediate)[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, (intermediate)[k].Value)
		}
		output := reducef((intermediate)[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
}

// readIntermediates reads file "mr-fileId-rIdx" and gets intermediate
func readIntermediates(fileId int, reduceId int) []KeyValue {
	fileName := fmt.Sprintf("mr-%v-%v", fileId, reduceId)
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("cann't open file: %v\n", fileName)
	}
	dec := json.NewDecoder(file)
	kva := make([]KeyValue, 0)
	for {
		var kv KeyValue
		err = dec.Decode(&kv)
		if err != nil {
			break
		}
		kva = append(kva, kv)
	}
	file.Close()
	return kva
}

func (worker *AWorker) joinReduceTask(rIdx int) {
	args := ReduceTaskJoinArgs{}
	args.rIdx = rIdx
	args.WorkerId = worker.workerId

	reply := ReduceTaskJoinReply{}
	call("Coordinator.JoinReduceTask", &args, &reply)

	if reply.Accept {
		worker.logPrintf("accepted\n")
	} else {
		worker.logPrintf("not accepted\n")
	}
}

func (worker *AWorker) executeReduce(reply *ReduceTaskReply) {
	outName := fmt.Sprintf("mr-out-%v", reply.RIndex)
	// ofile, err := os.create(outName)
	intermediate := make([]KeyValue, 0)
	for i := 0; i < reply.FileCount; i++ {
		worker.logPrintf("generating intermediates on cluster %v\n", i)
		// The way to append an array to another array.
		intermediate = append(intermediate, readIntermediates(i, reply.RIndex)...)
	}
	worker.logPrintf("total intermediate count %v\n", len(intermediate))
	tempFile, err := os.CreateTemp(".", "mrtemp")
	if err != nil {
		worker.logPrintf("cannot create tempFile for %v\n", outName)
	}
	reduceKVSlice(intermediate, worker.reducef, tempFile)
	tempFile.Close()
	err = os.Rename(tempFile.Name(), outName)
	if err != nil {
		worker.logPrintf("rename tempFile failed for %v\n", outName)
	}
	worker.joinReduceTask(reply.RIndex)
}

func (worker *AWorker) process() {
	if worker.allDone {
		// exit
	}
	if !worker.mapOrReduce {
		// process map task
		reply := worker.askMapTask()
		if reply == nil {
			// switch to reduce mode
			worker.mapOrReduce = true
		} else {
			if reply.FileId == -1 {
				// no available tasks for now
			} else {
				// must execute
				worker.executeMap(reply)
			}
		}
	}

	if worker.mapOrReduce {
		// process reduce task
		reply := worker.askReduceTask()
		if reply == nil {
			// all done
			// exit
			worker.allDone = true
		} else {
			if reply.RIndex == -1 {
				// no available tasks for now...
			} else {
				// must execute
				worker.executeReduce(reply)
			}
		}
	}

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
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
