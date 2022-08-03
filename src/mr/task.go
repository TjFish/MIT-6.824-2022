package mr

import (
	"container/list"
	"fmt"
	"sync"
	"time"
)

type TaskWorker interface {
	doTask(task Task)
	getTaskType() string
}

type Task struct {
	TaskNum   int
	TaskType  string
	Status    string
	TaskToken int64

	InputFileName string
	NReduce       int
	MapTaskTotal  int
}

func start(worker TaskWorker) {
	for {
		// 获取任务
		result := applyForTask(worker.getTaskType())
		switch result.Status {
		case "doing":
			task := result.Task
			worker.doTask(task)
			notifyTaskDone(task)
			break
		case "wait":
			fmt.Printf("wait for 3s\n")
			time.Sleep(time.Second * 3)
			break
		case "done":
			fmt.Printf("map task done\n")
			return
		}
	}
}

func applyForTask(taskType string) TaskApplyReply {
	args := TaskApplyArgs{TaskType: taskType}
	reply := TaskApplyReply{}
	ok := call("Coordinator.ApplyForTask", &args, &reply)
	if ok {
		//fmt.Printf("%+v \n", reply)
		return reply
	} else {
		fmt.Printf("call failed!\n")
	}
	return reply
}

func notifyTaskDone(task Task) {
	args := TaskDoneArgs{TaskType: task.TaskType, TaskNum: task.TaskNum, TaskToken: task.TaskToken}
	reply := TaskDoneReply{}
	ok := call("Coordinator.NotifyTaskDone", &args, &reply)
	if ok {
		//fmt.Printf("task done", reply)
	} else {
		fmt.Printf("call failed!\n")
	}
}

type TaskCoordinator struct {
	tasks           []*Task
	waitingTaskList *list.List
	doneTaskList    *list.List
	lock            sync.Mutex
}

func makeTaskCoordinator(tasks []*Task) (c *TaskCoordinator) {
	c = &TaskCoordinator{}
	c.tasks = tasks
	c.waitingTaskList = list.New()
	c.doneTaskList = list.New()
	c.lock = sync.Mutex{}

	for i := 0; i < len(tasks); i++ {
		tasks[i].TaskNum = i
		tasks[i].Status = "wait"
		c.waitingTaskList.PushBack(c.tasks[i])
	}
	return c
}

func (c *TaskCoordinator) ApplyForTask(args *TaskApplyArgs, reply *TaskApplyReply) error {
	c.lock.Lock()
	if c.waitingTaskList.Len() == 0 {
		//所有任务已经执行完毕
		//fmt.Printf("%+v\n", c.doneTaskList.Len())
		if c.doneTaskList.Len() < len(c.tasks) {
			reply.Status = "wait"
		}
		if c.doneTaskList.Len() == len(c.tasks) {
			reply.Status = "done"
			fmt.Println("all task done")
		}
	} else {
		task := c.waitingTaskList.Front().Value.(*Task)
		task.Status = "doing"
		task.TaskToken = time.Now().Unix()
		reply.Task = *task
		reply.Status = "doing"
		// 删除等待队列中的任务
		c.waitingTaskList.Remove(c.waitingTaskList.Front())
		// 延迟10s后检查是否完成，否则重新分配
		go c.retryAfter(task.TaskNum, time.Second*10)
	}
	c.lock.Unlock()
	return nil
}

func (c *TaskCoordinator) NotifyTaskDone(args *TaskDoneArgs, reply *TaskDoneReply) error {
	c.lock.Lock()
	taskNum := args.TaskNum
	task := c.tasks[taskNum]
	// 只有持有Token一致的才能完成
	if task.TaskToken == args.TaskToken {
		task.Status = "done"
		c.doneTaskList.PushBack(task)
		fmt.Printf("[%v/%v] task done %+v\n", c.doneTaskList.Len(), len(c.tasks), task)
	}
	c.lock.Unlock()
	return nil
}

func (c *TaskCoordinator) Done() bool {
	c.lock.Lock()
	if c.doneTaskList.Len() == len(c.tasks) {
		c.lock.Unlock()
		return true
	}
	c.lock.Unlock()
	return false
}

func (c *TaskCoordinator) retryAfter(taskNum int, d time.Duration) {
	timer := time.After(d)
	<-timer
	c.lock.Lock()
	task := c.tasks[taskNum]
	if task.Status == "done" {
		c.lock.Unlock()
		return
	}
	task.Status = "wait"
	task.TaskToken = 0
	c.waitingTaskList.PushBack(c.tasks[taskNum])
	c.lock.Unlock()
	return
}
