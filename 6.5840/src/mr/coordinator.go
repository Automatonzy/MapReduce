package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// TaskStatus represents the status of a task.
type TaskStatus int

const (
	Idle TaskStatus = iota
	InProgress
	Completed
)

// Task represents a Map or Reduce task.
type Task struct {
	ID        int
	Type      string // "map" or "reduce"
	Status    TaskStatus
	InputFile string
	StartTime time.Time
}

type Coordinator struct {
	mu          sync.Mutex
	mapTasks    []Task
	reduceTasks []Task
	nReduce     int
	nMap        int
	mapDone     bool
	reduceDone  bool
}

// GetTask handles a worker's request for a task.
func (c *Coordinator) GetTask(args *TaskRequest, reply *TaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.mapDone {
		for i := range c.mapTasks {
			if c.mapTasks[i].Status == Idle {
				c.mapTasks[i].Status = InProgress
				c.mapTasks[i].StartTime = time.Now()
				reply.TaskType = MapTask
				reply.TaskID = c.mapTasks[i].ID
				reply.InputFile = c.mapTasks[i].InputFile
				reply.NReduce = c.nReduce
				return nil
			}
		}
		reply.TaskType = WaitTask
		return nil
	}

	if !c.reduceDone {
		for i := range c.reduceTasks {
			if c.reduceTasks[i].Status == Idle {
				c.reduceTasks[i].Status = InProgress
				c.reduceTasks[i].StartTime = time.Now()
				reply.TaskType = ReduceTask
				reply.TaskID = c.reduceTasks[i].ID
				reply.NMap = c.nMap
				return nil
			}
		}
		reply.TaskType = WaitTask
		return nil
	}

	reply.TaskType = ExitTask
	return nil
}

// ReportTask handles a worker's notification that a task is finished.
func (c *Coordinator) ReportTask(args *ReportRequest, reply *ReportReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if args.TaskType == MapTask {
		if args.TaskID >= 0 && args.TaskID < len(c.mapTasks) && c.mapTasks[args.TaskID].Status == InProgress {
			if args.Success {
				c.mapTasks[args.TaskID].Status = Completed
			} else {
				c.mapTasks[args.TaskID].Status = Idle // Re-assign if failed
			}
		}
	} else if args.TaskType == ReduceTask {
		if args.TaskID >= 0 && args.TaskID < len(c.reduceTasks) && c.reduceTasks[args.TaskID].Status == InProgress {
			if args.Success {
				c.reduceTasks[args.TaskID].Status = Completed
			} else {
				c.reduceTasks[args.TaskID].Status = Idle // Re-assign if failed
			}
		}
	}

	c.checkCompletion()
	return nil
}

func (c *Coordinator) checkCompletion() {
	if !c.mapDone {
		allMapCompleted := true
		for _, task := range c.mapTasks {
			if task.Status != Completed {
				allMapCompleted = false
				break
			}
		}
		if allMapCompleted {
			c.mapDone = true
		}
	}

	if c.mapDone && !c.reduceDone {
		allReduceCompleted := true
		for _, task := range c.reduceTasks {
			if task.Status != Completed {
				allReduceCompleted = false
				break
			}
		}
		if allReduceCompleted {
			c.reduceDone = true
		}
	}
}

func (c *Coordinator) handleTimeouts() {
	for {
		time.Sleep(5 * time.Second) // Check every 5 seconds
		c.mu.Lock()
		if c.Done() {
			c.mu.Unlock()
			return
		}

		timeout := 10 * time.Second
		if !c.mapDone {
			for i := range c.mapTasks {
				if c.mapTasks[i].Status == InProgress && time.Since(c.mapTasks[i].StartTime) > timeout {
					log.Printf("Map task %d timed out. Resetting to Idle.", i)
					c.mapTasks[i].Status = Idle
				}
			}
		} else if !c.reduceDone {
			for i := range c.reduceTasks {
				if c.reduceTasks[i].Status == InProgress && time.Since(c.reduceTasks[i].StartTime) > timeout {
					log.Printf("Reduce task %d timed out. Resetting to Idle.", i)
					c.reduceTasks[i].Status = Idle
				}
			}
		}
		c.mu.Unlock()
	}
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
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.mapDone && c.reduceDone
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		mapTasks:    make([]Task, len(files)),
		reduceTasks: make([]Task, nReduce),
		nReduce:     nReduce,
		nMap:        len(files),
		mapDone:     false,
		reduceDone:  false,
	}

	for i, file := range files {
		c.mapTasks[i] = Task{ID: i, Type: MapTask, Status: Idle, InputFile: file}
	}

	for i := 0; i < nReduce; i++ {
		c.reduceTasks[i] = Task{ID: i, Type: ReduceTask, Status: Idle}
	}

	log.Printf("Coordinator started with %d map tasks and %d reduce tasks.", len(c.mapTasks), len(c.reduceTasks))

	c.server()
	go c.handleTimeouts()

	return &c
}
