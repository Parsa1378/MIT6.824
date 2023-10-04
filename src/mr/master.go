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

type ByKey []KeyValue

const (
	Idle      = 0
	InProcess = 1
	Finished  = 2
)

// type TaskStatus int
type Master struct {
	nMap       int
	nReduce    int
	files      []string
	fMaps      int
	logMaps    []int //0:not allocated, 1:waiting, 2:finished
	fReduce    int
	logReduces []int
	mu         sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//

func (m *Master) FinishedMap(args *TaskArgs, reply *TaskReply) error {
	m.mu.Lock()
	m.logMaps[args.MapTaskNum] = Finished
	m.fMaps++
	m.mu.Unlock()
	return nil
}

func (m *Master) FinishReduce(args *TaskArgs, reply *TaskReply) error {
	m.mu.Lock()
	m.logReduces[args.ReduceTaskNum] = Finished
	m.fReduce++
	m.mu.Unlock()
	return nil
}

func (m *Master) HandleTask(args *TaskArgs, reply *TaskReply) error {
	m.mu.Lock()
	if m.fMaps < m.nMap {
		allocate := -1
		for i := 0; i < m.nMap; i++ {
			if m.logMaps[i] == 0 {
				allocate = i
				break
			}
			if allocate == -1 {
				reply.Type = Waiting
				m.mu.Unlock()
			} else {
				reply.Type = MapTask
				reply.Filename = m.files[allocate]
				reply.MapTaskNuumber = allocate
				m.logMaps[allocate] = InProcess
				m.mu.Unlock()
				go func() {
					time.Sleep(time.Duration(10) * time.Second)
					m.mu.Lock()
					if m.logMaps[allocate] == InProcess {
						m.logMaps[allocate] = Idle
					}
					m.mu.Unlock()
				}()
			}
		}
	} else if m.nMap == m.fMaps && m.fReduce < m.nReduce {
		allocate := -1
		for i := 0; i < m.nReduce; i++ {
			if m.logReduces[i] == 0 {
				allocate = i
				break
			}
			if allocate == -1 {
				reply.Type = Waiting
				m.mu.Unlock()
			} else {
				reply.Type = ReduceTask
				reply.Filename = m.files[allocate]
				reply.ReduceTaskNumber = allocate
				m.logReduces[allocate] = InProcess
				m.mu.Unlock()
				go func() {
					time.Sleep(time.Duration(10) * time.Second)
					m.mu.Lock()
					if m.logReduces[allocate] == InProcess {
						m.logReduces[allocate] = Idle
					}
					m.mu.Unlock()
				}()
			}
		}
	} else {
		reply.Type = Done
		m.mu.Unlock()
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
func (m *Master) Done() bool {

	// Your code here.
	ret := m.fMaps == m.fReduce

	return ret
}

// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.

	m.server()
	return &m
}
