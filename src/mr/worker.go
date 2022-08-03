package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func tempName(fileName string) string {
	return "temp-" + fileName
}

func reduceName(mapTaskNum int, reduceTaskNum int) string {
	return "mr-im-" + strconv.Itoa(mapTaskNum) + "-" + strconv.Itoa(reduceTaskNum)
}

func outName(reduceTaskNum int) string {
	return "mr-out-" + strconv.Itoa(reduceTaskNum)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	mapTaskWorker := MapTaskWorker{mapf}
	start(&mapTaskWorker)

	reduceTaskWorker := ReduceTaskWorker{reducef: reducef}
	start(&reduceTaskWorker)
}

//
// example function to show how to make an RPC call to the coordinator.
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

type MapTaskWorker struct {
	mapf func(string, string) []KeyValue
}

func (w *MapTaskWorker) doTask(task Task) {
	// Your code here.
	fileName := task.InputFileName
	nReduce := task.NReduce
	mapTaskNum := task.TaskNum

	// 执行map函数
	content, _ := ioutil.ReadFile(fileName)
	kvs := w.mapf(fileName, string(content))

	// 创建中间文件
	reduceFiles := make([]*os.File, nReduce)
	for i := 0; i < nReduce; i++ {
		reduceFileName := reduceName(mapTaskNum, i)
		reduceFile, _ := ioutil.TempFile("", tempName(reduceFileName))
		reduceFiles[i] = reduceFile
	}

	// 写入中间文件
	for _, kv := range kvs {
		reduceTaskNum := ihash(kv.Key) % nReduce
		reduceFile := reduceFiles[reduceTaskNum]

		enc := json.NewEncoder(reduceFile)
		err := enc.Encode(&kv)
		if err != nil {
			log.Fatal("Encode error:", err)
		}
	}
	// 重命名中间文件
	for i := 0; i < nReduce; i++ {
		reduceFile := reduceFiles[i]
		reduceFile.Close()
		os.Rename(reduceFile.Name(), reduceName(mapTaskNum, i))
	}
}

func (*MapTaskWorker) getTaskType() string {
	return "map"
}

type ReduceTaskWorker struct {
	reducef func(string, []string) string
}

func (w *ReduceTaskWorker) doTask(task Task) {
	// Your code here.
	reduceTaskNum := task.TaskNum
	mapTaskTotal := task.MapTaskTotal

	// 读取中间文件的key/value到keyMap中
	kvs := make(map[string][]string)
	for i := 0; i < mapTaskTotal; i++ {
		reduceFileName := reduceName(i, reduceTaskNum)
		readReduceFile(reduceFileName, kvs)
	}

	// 创建输出文件
	outFileName := outName(reduceTaskNum)
	outFile, _ := ioutil.TempFile("", tempName(outFileName))

	// 执行reduce函数
	for key, values := range kvs {
		result := w.reducef(key, values)
		line := fmt.Sprintf("%v %v", key, result)
		fmt.Fprintln(outFile, line)
	}

	os.Rename(outFile.Name(), outFileName)
	outFile.Close()
}

func (*ReduceTaskWorker) getTaskType() string {
	return "reduce"
}

func readReduceFile(reduceFileName string, keyMap map[string][]string) {
	reduceFile, err := os.OpenFile(reduceFileName, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		log.Fatal("Open reduceFile error:", err)
	}
	dec := json.NewDecoder(reduceFile)
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			break
		}
		keyMap[kv.Key] = append(keyMap[kv.Key], kv.Value)
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
