package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"

// 原有结构保持不变
type KeyValue struct {
	Key   string
	Value string
}

func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// Worker主逻辑：循环请求任务并处理
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {
		// 1. 请求任务
		task, ok := requestTask()
		if !ok {
			log.Printf("请求任务失败，重试...")
			continue
		}

		// 2. 处理任务
		switch task.TaskType {
		case MapTask:
			// 后续实现Map任务处理
			log.Printf("收到Map任务 #%d，文件：%s", task.TaskID, task.InputFile)
			// 处理完成后汇报
			reportTask(MapTask, task.TaskID, true)
		case ReduceTask:
			// 后续实现Reduce任务处理
			log.Printf("收到Reduce任务 #%d", task.TaskID)
			// 处理完成后汇报
			reportTask(ReduceTask, task.TaskID, true)
		case WaitTask:
			// 等待一段时间后重试
			log.Printf("暂无任务，等待...")
			continue
		case ExitTask:
			// 退出 Worker
			log.Printf("所有任务完成，退出")
			return
		}
	}
}

// 向Coordinator请求任务
func requestTask() (TaskReply, bool) {
	args := TaskRequest{}
	reply := TaskReply{}
	ok := call("Coordinator.GetTask", &args, &reply)
	return reply, ok
}

// 向Coordinator汇报任务完成情况
func reportTask(taskType string, taskID int, success bool) bool {
	args := ReportRequest{
		TaskType: taskType,
		TaskID:   taskID,
		Success:  success,
	}
	reply := ReportReply{}
	return call("Coordinator.ReportTask", &args, &reply)
}

// 原有代码保持不变
func CallExample() {
	args := ExampleArgs{X: 99}
	reply := ExampleReply{}
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

func call(rpcname string, args interface{}, reply interface{}) bool {
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