package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
)

const (
	MAP_TASK_ID = "MAP"
	REDUCE_TASK_ID = "REDUCE"
)

type MapTask struct {
	fileName string
}

type ReduceTask struct {
	id int
}

type Coordinator struct {
	// Your definitions here.
	mu sync.Mutex
	files []string
	nReduce int

	//in progress map tasks
	mapsInProgress map[string]MapTask

	//in progress reduce tasks
	reducesInProgress map[string]ReduceTask

	//queue of map tasks and reduce tasks
	mapTasks []MapTask
	reduceTasks []ReduceTask
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) getTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	//assign a map task if there is one in the queue
	if len(c.mapTasks) != 0 {
		reply.containsTask = true
		task := c.mapTasks[0]
		reply.taskTarget = task.fileName
		reply.taskID = MAP_TASK_ID
		c.mapTasks = c.mapTasks[1:]
		c.mapsInProgress[args.workerID] = task
	} else if len(c.mapsInProgress) == 0 && len(c.reduceTasks) != 0 {
		//we can give a reduce task since map tasks are empty,
		//no in-progress map tasks, and there is a reduce task to give
		reply.containsTask = true
		task := c.reduceTasks[0]
		reply.taskTarget = strconv.Itoa(task.id)
		reply.taskID = REDUCE_TASK_ID
		c.reduceTasks = c.reduceTasks[1:]
		c.reducesInProgress[args.workerID] = task
	} else {
		//otherwise don't give a task
		reply.containsTask = false
	}
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	//we will be done when there are no outstanding map or reduce tasks
	//and no in-progress map or reduce tasks

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.


	c.server()
	return &c
}
