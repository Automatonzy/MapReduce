package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type Coordinator struct {
	// 后续实现需要的字段（暂不实现）
}

// RPC处理器：处理Worker的任务请求
func (c *Coordinator) GetTask(args *TaskRequest, reply *TaskReply) error {
	// 后续实现任务分配逻辑
	// 暂时返回等待任务
	reply.TaskType = WaitTask
	return nil
}

// RPC处理器：处理Worker的任务汇报
func (c *Coordinator) ReportTask(args *ReportRequest, reply *ReportReply) error {
	// 后续实现任务状态更新逻辑
	return nil
}

// 原有示例方法保持不变
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

func (c *Coordinator) Done() bool {
	ret := false
	// 后续实现完成判断逻辑
	return ret
}

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	// 后续实现初始化逻辑
	c.server()
	return &c
}
