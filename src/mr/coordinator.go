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

type Coordinator struct {
	// Your definitions here.
	Files            []string
	Filesize         int
	Isalloc_map      []bool
	Whenalloc_map    []int64
	Finished_map     []bool
	Finishedsize_map int

	Nreduce             int
	Isalloc_reduce      []bool
	Whenalloc_reduce    []int64
	Finished_reduce     []bool
	Finishedsize_reduce int

	Finishedsize_mutex sync.RWMutex
	Alloc_mutex        sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) AllocMap(args *AllocMapArgs, reply *AllocMapReply) error {
	if args.Fileno != -1 {
		if !c.Finished_map[args.Fileno] {
			c.Finished_map[args.Fileno] = true
			c.Finishedsize_mutex.Lock()
			c.Finishedsize_map++
			c.Finishedsize_mutex.Unlock()
		}
		c.Finishedsize_mutex.RLock()
		if c.Finishedsize_map == c.Filesize {
			c.Finishedsize_mutex.RUnlock()
			reply.Eof = true
			reply.Fileno = -1
			return nil
		}
		c.Finishedsize_mutex.RUnlock()
	}
	c.Alloc_mutex.Lock()
	for i := 0; i < c.Filesize; i++ {
		if !c.Isalloc_map[i] || (!c.Finished_map[i] && time.Now().Unix()-c.Whenalloc_map[i] > 10) {
			c.Isalloc_map[i] = true
			c.Whenalloc_map[i] = time.Now().Unix()
			c.Alloc_mutex.Unlock()
			reply.Filename = c.Files[i]
			reply.Fileno = i
			reply.Nreduce = c.Nreduce
			reply.Eof = false
			return nil
		}
	}
	c.Alloc_mutex.Unlock()
	c.Finishedsize_mutex.RLock()
	reply.Eof = c.Finishedsize_map == c.Filesize
	c.Finishedsize_mutex.RUnlock()
	reply.Fileno = -1
	return nil
}

func (c *Coordinator) AllocReduce(args *AllocReduceArgs, reply *AllocReduceReply) error {
	if args.Reduceno != -1 {
		if !c.Finished_reduce[args.Reduceno] {
			c.Finished_reduce[args.Reduceno] = true
			c.Finishedsize_mutex.Lock()
			c.Finishedsize_reduce++
			c.Finishedsize_mutex.Unlock()
		}
		c.Finishedsize_mutex.RLock()
		if c.Finishedsize_reduce == c.Nreduce {
			c.Finishedsize_mutex.RUnlock()
			reply.Reduced = true
			reply.Reduceno = -1
			return nil
		}
		c.Finishedsize_mutex.RUnlock()
	}
	c.Alloc_mutex.Lock()
	for i := 0; i < c.Nreduce; i++ {
		if !c.Isalloc_reduce[i] || (!c.Finished_reduce[i] && time.Now().Unix()-c.Whenalloc_reduce[i] > 10) {
			c.Isalloc_reduce[i] = true
			c.Whenalloc_reduce[i] = time.Now().Unix()
			c.Alloc_mutex.Unlock()
			reply.Reduceno = i
			reply.Reduced = false
			reply.Filesize = c.Filesize
			return nil
		}
	}
	c.Alloc_mutex.Unlock()
	c.Finishedsize_mutex.RLock()
	reply.Reduced = c.Finishedsize_reduce == c.Nreduce
	c.Finishedsize_mutex.RUnlock()
	reply.Reduceno = -1
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
// if the entire job has Finished_map.
//
func (c *Coordinator) Done() bool {
	// Your code here.
	c.Finishedsize_mutex.RLock()
	done := c.Finishedsize_reduce == c.Nreduce
	c.Finishedsize_mutex.RUnlock()
	return done
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// Nreduce is the number of reduce tasks to use.
//
func MakeCoordinator(Files []string, Nreduce int) *Coordinator {
	c := Coordinator{}
	c.Files = Files
	c.Nreduce = Nreduce
	c.Filesize = len(Files)
	c.Isalloc_map = make([]bool, c.Filesize)
	c.Whenalloc_map = make([]int64, c.Filesize)
	c.Finished_map = make([]bool, c.Filesize)
	c.Isalloc_reduce = make([]bool, Nreduce)
	c.Whenalloc_reduce = make([]int64, Nreduce)
	c.Finished_reduce = make([]bool, Nreduce)
	// Your code here.

	c.server()
	return &c
}
