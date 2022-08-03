package mr

import (
	"log"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	files                 []string
	nReduce               int
	mapTaskCoordinator    *TaskCoordinator
	reduceTaskCoordinator *TaskCoordinator
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) ApplyForTask(args *TaskApplyArgs, reply *TaskApplyReply) error {
	if args.TaskType == "map" {
		c.mapTaskCoordinator.ApplyForTask(args, reply)
	} else if args.TaskType == "reduce" {
		c.reduceTaskCoordinator.ApplyForTask(args, reply)
	}
	return nil
}

func (c *Coordinator) NotifyTaskDone(args *TaskDoneArgs, reply *TaskDoneReply) error {
	if args.TaskType == "map" {
		c.mapTaskCoordinator.NotifyTaskDone(args, reply)
	} else if args.TaskType == "reduce" {
		c.reduceTaskCoordinator.NotifyTaskDone(args, reply)
	}
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
	if c.mapTaskCoordinator.Done() && c.reduceTaskCoordinator.Done() {
		ret = true
	}
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// NReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{files: files, nReduce: nReduce}
	mapTasks := make([]*Task, len(files))
	for i := 0; i < len(files); i++ {
		task := Task{InputFileName: files[i], NReduce: nReduce, TaskType: "map", TaskNum: i}
		mapTasks[i] = &task
	}
	c.mapTaskCoordinator = makeTaskCoordinator(mapTasks)

	reduceTasks := make([]*Task, nReduce)
	for i := 0; i < nReduce; i++ {
		task := Task{MapTaskTotal: len(files), TaskType: "reduce", TaskNum: i}
		reduceTasks[i] = &task
	}
	c.reduceTaskCoordinator = makeTaskCoordinator(reduceTasks)
	// Your code here.

	c.server()
	return &c
}
