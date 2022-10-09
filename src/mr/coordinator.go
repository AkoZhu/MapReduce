package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

const maxTaskTime = 10 // seconds.

// MapTaskState Map Task State
type MapTaskState struct {
	beginSecond int64
	workerId    int
	fileId      int
}

// ReduceTaskState Reduce Task State
type ReduceTaskState struct {
	beginSecond int64
	workerId    int
	fileId      int
}

type Coordinator struct {
	// Your definitions here.
	fileNames []string // file name of original files
	nReduce   int      // number of reduce task we separate.

	curWorkerId int

	unIssuedMapTasks *BlockQueue
	issuedMapTasks   *MapSet
	issuedMapMutex   sync.Mutex

	unIssuedReduceTasks *BlockQueue
	issuedReduceTasks   *MapSet
	issuedReduceMutex   sync.Mutex

	// task states, used to mark states for all tasks.
	mapTasks    []MapTaskState
	reduceTasks []ReduceTaskState

	// states
	mapDone bool
	allDone bool
}

// Your code here -- RPC handlers for the worker to call.

type MapTaskArgs struct {
	// -1 if it doesn't have one.
	WorkerId int
}

type MapTaskReply struct {
	// work passes this to the os package
	FileName string

	// marks a unique file for mapping
	// gives -1 for no more fileId
	FileId int

	// for reduce tasks
	NReduce int

	// assign worker id as this reply is the first sent to workers.
	WorkerId int

	// whether this kind of tasks are all done
	// If not, and fileId is -1, the worker waits.
	AllDone bool
}

func mapDoneProcess(reply *MapTaskReply) {
	log.Printf("all map tasks complete, telling workers to switch to reduce mode")
	// If map tasks are done, we mark FileId = -1 and mark allDone = true to worker.
	reply.FileId = -1
	reply.AllDone = true
}

// GiveMapTask : Call it from worker and get response, which includes task states like startTime, fileId, and workerId
func (c *Coordinator) GiveMapTask(args *MapTaskArgs, reply *MapTaskReply) error {
	if args.WorkerId == -1 {
		// If not, use curWorkerId.
		reply.WorkerId = c.curWorkerId
		c.curWorkerId++
	} else {
		// If args has WorkerId, use this as workerId.
		reply.WorkerId = args.WorkerId
	}
	log.Printf("worker %v asks for a map task\n", reply.WorkerId)

	c.issuedMapMutex.Lock()

	// 1. If current Map state is Done.
	// After it reads the state, we need to release a lock.
	if c.mapDone {
		c.issuedMapMutex.Unlock()
		mapDoneProcess(reply)
		return nil
	}

	// 2. If current Map state is not done but Map tasks are all finished.
	// We mark mapDone and start reduce tasks.
	if c.unIssuedMapTasks.Size() == 0 && c.issuedMapTasks.Size() == 0 {
		c.issuedMapMutex.Unlock()
		mapDoneProcess(reply)
		c.prepareAllReduceTasks()
		c.mapDone = true
		return nil
	}

	// 3. If we still have Map tasks.
	log.Printf("%v unissued map tasks %v issued map tasks at hand\n", c.unIssuedMapTasks.Size(), c.issuedMapTasks.Size())

	c.issuedMapMutex.Unlock() // release lock to allow unissued update after we use issuedMapTasks.
	curTime := getNowTimeSecond()
	res, err := c.unIssuedMapTasks.PopBack()
	var fileId int

	// Try to get tasks from unIssuedTask list.
	if err != nil {
		log.Printf("no map task yet, let workers wait...")
		fileId = -1
	} else {
		// Get fileId from the BlockQueue.
		// Convert interface into int type.
		fileId = res.(int)
		c.issuedMapMutex.Lock()
		// Change Task state in MapTask Set, we need to lock.
		reply.FileName = c.fileNames[fileId]
		c.mapTasks[fileId].beginSecond = curTime
		c.mapTasks[fileId].workerId = reply.WorkerId
		// Add issuedTask into issuedMapTask.
		c.issuedMapTasks.Insert(fileId)
		c.issuedMapMutex.Unlock()
		log.Printf("giving map task %v on file %v at second %v\n", fileId, reply.FileId, curTime)
	}
	// mark which file
	reply.FileId = fileId
	reply.AllDone = false
	// mark the number of reduce task we separate. It is determined.
	reply.NReduce = c.nReduce

	return nil
}

func getNowTimeSecond() int64 {
	return time.Now().UnixNano() / int64(time.Second)
}

// MapTaskJoinArgs : We need to know which worker is timed out and the fileId it works on.
type MapTaskJoinArgs struct {
	FileId   int
	WorkerId int
}

// MapTaskJoinReply : we need to know if we successfully join in a stale task.
type MapTaskJoinReply struct {
	Accept bool
}

// JoinMapTask : Join in the MapTask BlockQueue after map task is done(timeout or in time)
func (c *Coordinator) JoinMapTask(args *MapTaskJoinArgs, reply *MapTaskJoinReply) error {
	// Check if there is a worker has timed out.
	log.Printf("got join request from worker %v on file %v \n", args.WorkerId, args.FileId)

	c.issuedMapMutex.Lock()

	curTime := getNowTimeSecond()
	taskTime := c.mapTasks[args.FileId].beginSecond
	// If fileId is not in the issuedTask, there is a wrong, and we don't reassign the task.
	if !c.issuedMapTasks.Has(args.FileId) {
		log.Printf("task abandoned or does not exists, ignoring...")
		c.issuedMapMutex.Unlock()
		reply.Accept = false
		return nil
	}
	// If the assigned workerId is not the same as args.WorkerId, there is a wrong.
	if c.mapTasks[args.FileId].workerId != args.WorkerId {
		log.Printf("map task belongs to worker %v not this %v, ignoring...", c.mapTasks[args.FileId].workerId,
			args.WorkerId)
		c.issuedMapMutex.Unlock()
		reply.Accept = false
		return nil
	}

	// If there is no wrong, we start to reassign the task.
	if curTime-taskTime > maxTaskTime {
		log.Printf("task exceeds max wait time, abandoning...")
		reply.Accept = false
		// Reassign the task and put FileId into unIssuedMapTasks Set.
		c.unIssuedMapTasks.PutFront(args.FileId)
		// I think we need to remove args.FIleId from issuedMapTasks in this stage.
	} else {
		log.Printf("task within max wait time, accepting...")
		reply.Accept = true
		c.issuedMapTasks.Remove(args.FileId)
	}

	c.issuedMapMutex.Unlock()
	return nil
}

type ReduceTaskArgs struct {
	WorkerId int // assigned to which worker.
}

type ReduceTaskReply struct {
	RIndex    int  // rIdx, the index of current reduce task rIdx \in [0, nReduce - 1]
	NReduce   int  // The total number of reduce task
	FileCount int  // The number of files in total. len(fileNames)
	AllDone   bool // AllDone flag.
}

// offer all reduce task ID into queue.
func (c *Coordinator) prepareAllReduceTasks() {
	for i := 0; i < c.nReduce; i++ {
		log.Printf("putting %vth reduce task into channel\n", i)
		c.unIssuedReduceTasks.PutFront(i) // get ready for unIssuedReduceTask Queue.
	}
}

func (c *Coordinator) GiveReduceTask(args *ReduceTaskArgs, reply *ReduceTaskReply) error {
	log.Printf("worker %v asks for a reduce task\n", args.WorkerId)
	c.issuedReduceMutex.Lock()

	// If all reduce tasks are done.
	if c.unIssuedReduceTasks.Size() == 0 && c.issuedReduceTasks.Size() == 0 {
		log.Printf("all reduce tasks complete, telling all workers to terminate")
		c.issuedMapMutex.Unlock()
		c.allDone = true
		reply.RIndex = -1
		reply.AllDone = true
		return nil
	}

	log.Printf("%v unissued reduce tasks, %v issued reduce tasks at hand\n", c.unIssuedReduceTasks.Size(), c.issuedReduceTasks.Size())
	c.issuedReduceMutex.Unlock()
	curTime := getNowTimeSecond()
	// Pop an unIssuedReduceTask
	res, err := c.unIssuedReduceTasks.PopBack()
	var rIdx int
	if err != nil {
		log.Printf("no reduce task yet, let work waits...")
		rIdx = -1
	} else {
		rIdx = res.(int)
		c.issuedReduceMutex.Lock()
		// Change the states in the reduceTasks map
		c.reduceTasks[rIdx].beginSecond = curTime
		c.reduceTasks[rIdx].workerId = args.WorkerId // assigned WorkerId
		// Add the rIdx into the issuedReduceTasks
		c.issuedReduceTasks.Insert(rIdx)
		c.issuedReduceMutex.Unlock()
		log.Printf("giving reduce task %v at second %v \n", rIdx, curTime)
	}

	reply.RIndex = rIdx
	reply.AllDone = false
	reply.NReduce = c.nReduce
	reply.FileCount = len(c.fileNames)
	return nil
}

// Manage rejoin the Reduce Task

type ReduceTaskJoinArgs struct {
	WorkerId int
	rIdx     int
}

type ReduceTaskJoinReply struct {
	Accept bool
}

func (c *Coordinator) JoinReduceTask(args *ReduceTaskJoinArgs, reply *ReduceTaskJoinReply) error {
	// Check the request from which worker and which reduce task.
	log.Printf("got rejoin request from worker %v in reduce task %v", args.WorkerId, args.rIdx)

	curTime := getNowTimeSecond()
	taskTime := c.reduceTasks[args.rIdx].beginSecond
	c.issuedReduceMutex.Lock()

	if !c.issuedReduceTasks.Has(args.rIdx) {
		log.Printf("task abandoned or does not exists, ignoring...")
		c.issuedReduceMutex.Unlock()
		reply.Accept = false
		return nil
	}

	if args.WorkerId != c.reduceTasks[args.rIdx].workerId {
		log.Printf("reduce task belongs to worker %v, not this %v, ignoring...", args.WorkerId, c.reduceTasks[args.rIdx].workerId)
		c.issuedReduceMutex.Unlock()
		reply.Accept = false
		return nil
	}

	if curTime-taskTime > maxTaskTime {
		log.Printf("reduce task exceeds max wait time, abandoning...")
		reply.Accept = false
		c.unIssuedReduceTasks.PutFront(args.rIdx)

	} else {
		log.Printf("reduce task within max wait time, accepting...")
		reply.Accept = true
		c.issuedReduceTasks.Remove(args.rIdx)
	}

	c.issuedReduceMutex.Unlock()
	return nil
}

// Remove timeout Map task from issuedMapTasks and add it into unIssuedMapTask Queue.
func (m *MapSet) removeTimeoutMapTasks(mapTasks []MapTaskState, unIssuedMapTasks *BlockQueue) {
	for fileId, issued := range m.mapbool {
		now := getNowTimeSecond()
		if issued {
			if now-mapTasks[fileId.(int)].beginSecond > maxTaskTime {
				log.Printf("worker %v on file %v abandoned due to timeout\n", mapTasks[fileId.(int)].workerId,
					fileId)
				m.mapbool[fileId.(int)] = false
				m.count++
				unIssuedMapTasks.PutFront(fileId.(int))
			}
		}
	}
}

// Remove timeout Reduce task from issuedReduceTasks and add it into unIssuedReduceTask Queue.
func (m *MapSet) removeTimeoutReduceTasks(reduceTasks []ReduceTaskState, unIssuedReduceTasks *BlockQueue) {

	for rIdx, issued := range m.mapbool {
		now := getNowTimeSecond()
		if issued {
			if now-reduceTasks[rIdx.(int)].beginSecond > maxTaskTime {
				log.Printf("worker %v on reduce task %v abandoned due to timeout\n", reduceTasks[rIdx.(int)].workerId,
					rIdx)
				m.mapbool[rIdx.(int)] = false
				m.count--
				unIssuedReduceTasks.PutFront(rIdx.(int))
			}
		}
	}
}

// remove both map tasks and reduce tasks if they are timeout.
func (c *Coordinator) removeTimeoutTasks() {
	log.Printf("removing timeout maptasks and reducetasks...")
	c.issuedMapMutex.Lock()
	c.issuedMapTasks.removeTimeoutMapTasks(c.mapTasks, c.unIssuedMapTasks)
	c.issuedMapMutex.Unlock()
	c.issuedReduceMutex.Lock()
	c.issuedReduceTasks.removeTimeoutReduceTasks(c.reduceTasks, c.unIssuedReduceTasks)
	c.issuedReduceMutex.Unlock()
}

// Infinite loop with sleep to remove timeout tasks.
func (c *Coordinator) loopRemoveTimeoutMapTasks() {
	for true {
		time.Sleep(2 * 1000 * time.Millisecond)
		c.removeTimeoutTasks()
	}
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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
	//ret := false

	// Your code here.
	if c.allDone {
		log.Printf("asked whether it is done, replying yes...")
	} else {
		log.Printf("asked whether it is done, replying no...")
	}

	return c.allDone
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	log.SetPrefix("coordinator: ")
	log.Printf("making coordinator")

	// Initialize all things in coordinator.
	c.fileNames = files
	c.nReduce = nReduce
	c.curWorkerId = 0
	c.mapTasks = make([]MapTaskState, len(files))
	c.reduceTasks = make([]ReduceTaskState, nReduce)
	c.unIssuedMapTasks = NewBlockqueue()
	c.issuedMapTasks = NewMapSet()
	c.unIssuedReduceTasks = NewBlockqueue()
	c.issuedReduceTasks = NewMapSet()
	c.allDone = false
	c.mapDone = false

	// start a thread that listens for RPCs from worker.go
	c.server()
	log.Printf("listening started....")
	go c.loopRemoveTimeoutMapTasks()

	// Initialize the unissuedMapTasks.
	// all are unissued map tasks
	// send to channel after everything else initializes
	log.Printf("file count %d\n", len(files))
	for i := 0; i < len(files); i++ {
		log.Printf("sending %vth file map task to channel\n", i)
		c.unIssuedMapTasks.PutFront(i)
	}

	return &c
}
