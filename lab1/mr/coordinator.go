package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

const (
	MapPhase = iota
	ReducePhase
	AllDone
)

const (
	Idle = iota
	Running
	Completed
)

type Coordinator struct {
	// Your definitions here.
	files           []string // 原始文本
	mapTaskCount    int
	reduceTaskCount int
	Mu              sync.Mutex
	taskPhase       int
	taskInfoHolder  map[int]*TaskInfo
}

type TaskInfo struct {
	taskState int
	taskId    int
	startTime time.Time
	taskFiles []string
}

func (c *Coordinator) addTask(taskInfo *TaskInfo) error {
	id := taskInfo.taskId
	c.taskInfoHolder[id] = taskInfo
	return nil
}

// Map任务最早创建
func (c *Coordinator) makeMapTask(files []string) {
	for index, file := range files {
		taskInfo := TaskInfo{
			taskState: Idle,
			taskId:    index,
			taskFiles: []string{file},
		}
		c.addTask(&taskInfo)
	}
}

// reduce任务创建
func (c *Coordinator) makeReduceTask() {
	for i := 0; i < 10; i++ {
		taskInfo := TaskInfo{
			taskState: Idle,
			taskId:    8 + i,
			taskFiles: []string{strconv.Itoa(i)},
		}
		c.addTask(&taskInfo)
	}
}

func (c *Coordinator) isMapsDone() bool {
	count := 0
	for _, taskinfo := range c.taskInfoHolder {
		if taskinfo.taskState == Completed {
			count++
		}
		if count == 8 {
			return true
		}
	}
	return false
}

func (c *Coordinator) isReducesDone() bool {
	count := 0
	for i := 8; i < 18; i++ {
		if c.taskInfoHolder[i].taskState == Completed {
			count++
		}
		if count == 10 {
			return true
		}
	}
	return false
}

// func (c *Coordinator) isTaskRunning(taskInfo TaskInfo) bool {
// 	if taskInfo.taskState == Running {
// 		return true
// 	}
// 	return false
// }

func (c *Coordinator) nextPhase() {
	if c.taskPhase == MapPhase {
		c.makeReduceTask()
		c.taskPhase = ReducePhase
		return
	}
	if c.taskPhase == ReducePhase {
		c.taskPhase = AllDone
		return
	}
}

func (c *Coordinator) AskTaskHandler(args *Args, reply *Reply) error {
	c.Mu.Lock()
	if c.taskPhase == MapPhase {
		if c.mapTaskCount < 8 {
			reply.TaskTypes = "map"
			taskId := 0
			for i := 0; i < 8; i++ {
				if c.taskInfoHolder[i].taskState == Idle {
					taskId = c.taskInfoHolder[i].taskId
				}
			}
			taskInfo := c.taskInfoHolder[taskId]
			taskInfo.startTime = time.Now()
			taskInfo.taskState = Running
			reply.TaskFile = taskInfo.taskFiles[0]
			reply.TaskId = taskInfo.taskId
			c.mapTaskCount++
			// c.mappingFiles = append(c.mappingFiles, c.files[0])
			// c.files = c.files[1:]

		} else {
			// 任务全部分发出去不一定全部已经成功
			reply.TaskTypes = "wait"
			// 检查任务是否全部完成
			// 若全部完成则执行下个阶段任务
			if c.isMapsDone() {
				c.nextPhase()
			}
		}
	}
	if c.taskPhase == ReducePhase {
		if c.reduceTaskCount < 10 {
			fmt.Println(c.reduceTaskCount)
			reply.TaskTypes = "reduce"
			taskId := 8
			for i := 8; i < 18; i++ {
				if c.taskInfoHolder[i].taskState == Idle {
					taskId = c.taskInfoHolder[i].taskId
				}
			}
			taskInfo := c.taskInfoHolder[taskId]
			taskInfo.startTime = time.Now()
			taskInfo.taskState = Running
			reply.TaskFile = taskInfo.taskFiles[0]
			reply.TaskId = taskInfo.taskId
			c.reduceTaskCount++
		} else {
			reply.TaskTypes = "wait"
			if c.isReducesDone() {
				c.nextPhase()
			}
		}
	}
	if c.taskPhase == AllDone {
		reply.TaskTypes = "exit"
	}
	c.Mu.Unlock()
	return nil
}

func (c *Coordinator) SubmitTaskHandler(args *Args, reply *Reply) error {
	c.Mu.Lock()
	id := args.TaskId
	// 不为running说明超时，worker可能crash
	if c.taskInfoHolder[id].taskState == Running {
		// 任务提交后：修改taskinfo
		if args.MapOrReduce == "map" {
			if args.TaskResult == SUCCESS {
				c.taskInfoHolder[id].taskState = Completed
			}
		}
		if args.MapOrReduce == "reduce" {
			if args.TaskResult == SUCCESS {
				c.taskInfoHolder[id].taskState = Completed
			}
		}
	}
	c.Mu.Unlock()
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
	if c.taskPhase == AllDone {
		ret = true
	}

	return ret
}

func (c *Coordinator) checkTaskCrash() {
	taskInfos := c.taskInfoHolder
	for {
		time.Sleep(time.Second)
		if c.taskPhase == MapPhase {
			for i := 0; i < 8; i++ {
				taskInfo := taskInfos[i]
				if taskInfo.taskState == Running {
					if time.Since(taskInfo.startTime) > 10*time.Second {
						c.Mu.Lock()
						c.taskInfoHolder[i].taskState = Idle
						c.mapTaskCount--
						c.Mu.Unlock()
					}
				}
			}
		}
		if c.taskPhase == ReducePhase {
			for i := 8; i < 18; i++ {
				taskInfo := taskInfos[i]
				if taskInfo.taskState == Running {
					if time.Since(taskInfo.startTime) > 10*time.Second {
						c.Mu.Lock()
						c.taskInfoHolder[i].taskState = Idle
						c.reduceTaskCount--
						c.Mu.Unlock()
					}
				}
			}
		}
		if c.taskPhase == AllDone {
			break
		}
	}
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	// Your code here.
	c.files = files
	c.mapTaskCount = 0
	c.reduceTaskCount = 0
	// map任务：8；reduce任务：10
	c.taskInfoHolder = make(map[int]*TaskInfo, 18)
	c.makeMapTask(files)
	c.taskPhase = MapPhase
	go c.checkTaskCrash()
	c.server()
	return &c
}
