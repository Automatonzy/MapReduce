package mr

import (
	"os"
	"strconv" // 导入strconv包
)

// 任务类型常量
const (
	MapTask    = "map"
	ReduceTask = "reduce"
	WaitTask   = "wait"
	ExitTask   = "exit"
)

// TaskRequest: Worker向Coordinator请求任务
type TaskRequest struct {
	// 可携带Worker标识等信息，可选
	WorkerID int
}

// TaskReply: Coordinator向Worker返回的任务信息
type TaskReply struct {
	TaskType   string // 任务类型：MapTask/ReduceTask/WaitTask/ExitTask
	TaskID     int    // 任务ID
	InputFile  string // 仅Map任务：输入文件名
	NReduce    int    // 总Reduce任务数（Map任务需要）
	NMap       int    // 总Map任务数（Reduce任务需要）
	ReduceFile string // 仅Reduce任务：输出文件名前缀
}

// ReportRequest: Worker向Coordinator汇报任务完成情况
type ReportRequest struct {
	TaskType string // 任务类型
	TaskID   int    // 任务ID
	Success  bool   // 是否成功完成
}

// ReportReply: Coordinator对汇报的响应
type ReportReply struct {
	// 通常不需要返回数据，仅确认收到
}

// 原有代码保持不变...
type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
