package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"sort"
	"time"
	"net/rpc"
	"hash/fnv"
)

// KeyValue 定义 Map/Reduce 中的键值对
type KeyValue struct {
	Key   string
	Value string
}

func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// Worker 主循环
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {
		task, ok := requestTask()
		if !ok {
			log.Printf("请求任务失败，重试...")
			time.Sleep(time.Second)
			continue
		}

		switch task.TaskType {
		case MapTask:
			doMap(task, mapf)
		case ReduceTask:
			doReduce(task, reducef)
		case WaitTask:
			time.Sleep(time.Second)
		case ExitTask:
			log.Printf("所有任务完成，退出 Worker")
			return
		}
	}
}

//
// Map 阶段：读取输入文件 -> mapf -> 分区写入中间文件
//
func doMap(task TaskReply, mapf func(string, string) []KeyValue) {
	filename := task.InputFile
	content, err := ioutil.ReadFile(filename)
	if err != nil {
		log.Fatalf("无法读取输入文件 %v: %v", filename, err)
	}

	kva := mapf(filename, string(content))

	// 创建 NReduce 个临时文件
	encoders := make([]*json.Encoder, task.NReduce)
	files := make([]*os.File, task.NReduce)
	for r := 0; r < task.NReduce; r++ {
		tmpfile, err := ioutil.TempFile("", "mr-tmp-*")
		if err != nil {
			log.Fatalf("无法创建临时文件: %v", err)
		}
		encoders[r] = json.NewEncoder(tmpfile)
		files[r] = tmpfile
	}

	// 写入对应 reduce 分区
	for _, kv := range kva {
		r := ihash(kv.Key) % task.NReduce
		err := encoders[r].Encode(&kv)
		if err != nil {
			log.Fatalf("写入中间文件失败: %v", err)
		}
	}

	// 原子重命名
	for r := 0; r < task.NReduce; r++ {
		oldname := files[r].Name()
		files[r].Close()
		newname := fmt.Sprintf("mr-%d-%d", task.TaskID, r)
		os.Rename(oldname, newname)
	}

	reportTask(MapTask, task.TaskID, true)
}

//
// Reduce 阶段：扫描所有 mr-*-%d 文件 -> 解码 -> 分组 -> reducef -> 输出
//
func doReduce(task TaskReply, reducef func(string, []string) string) {
	pattern := fmt.Sprintf("mr-*-%d", task.TaskID)
	files, err := filepath.Glob(pattern)
	if err != nil {
		log.Fatalf("扫描中间文件失败: %v", err)
	}

	kvs := make(map[string][]string)
	for _, fname := range files {
		file, err := os.Open(fname)
		if err != nil {
			log.Fatalf("无法打开中间文件 %v: %v", fname, err)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kvs[kv.Key] = append(kvs[kv.Key], kv.Value)
		}
		file.Close()
	}

	// 按 key 排序
	var keys []string
	for k := range kvs {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// 输出结果
	tmpfile, err := ioutil.TempFile("", "mr-out-tmp-*")
	if err != nil {
		log.Fatalf("无法创建 reduce 临时文件: %v", err)
	}
	for _, k := range keys {
		output := reducef(k, kvs[k])
		fmt.Fprintf(tmpfile, "%v %v\n", k, output)
	}
	tmpname := tmpfile.Name()
	tmpfile.Close()

	outname := fmt.Sprintf("mr-out-%d", task.TaskID)
	os.Rename(tmpname, outname)

	reportTask(ReduceTask, task.TaskID, true)
}

// 请求任务
func requestTask() (TaskReply, bool) {
	args := TaskRequest{}
	reply := TaskReply{}
	ok := call("Coordinator.GetTask", &args, &reply)
	return reply, ok
}

// 上报任务
func reportTask(taskType string, taskID int, success bool) bool {
	args := ReportRequest{
		TaskType: taskType,
		TaskID:   taskID,
		Success:  success,
	}
	reply := ReportReply{}
	return call("Coordinator.ReportTask", &args, &reply)
}

// RPC 调用封装
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
