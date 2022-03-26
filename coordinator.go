package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Task struct {
	Task_Type   string
	Task_Number int
	Status      string
	Path        string
}

type Coordinator struct {
	mapping_done bool
	map_tasks    []Task
	reduce_tasks []Task
	nReduce      int
	mu           sync.Mutex
}



func (c *Coordinator) checkCrash(i int, t string) {

	time.Sleep(10 * time.Second)
	c.mu.Lock()
	if t == "map" {
		if c.map_tasks[i].Status == "in progress" {
			c.map_tasks[i].Status = "available"
		}
	} else {
		if c.reduce_tasks[i].Status == "in progress" {
			c.reduce_tasks[i].Status = "available"
		}
	}
	c.mu.Unlock()
}

func (c *Coordinator) CheckMapDone() bool {
	c.mu.Lock()
	for _, t := range c.map_tasks {
		if t.Status != "done" {
			c.mu.Unlock()
			return false
		}
	}
	c.mu.Unlock()
	return true
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	// If all mapping tasks are done, we will give the worker a reduce task
	c.mu.Lock()
	if c.mapping_done {
		for i, t := range c.reduce_tasks {
			if t.Status == "available" {
				fmt.Println("Coordinator: Handing Reduce task ", i)
				c.reduce_tasks[i].Status = "in progress"
				reply.T = t
				reply.NReduce = c.nReduce
				reply.NMap = len(c.map_tasks)
				reply.Wait = false

				if reply.Wait == false {
					fmt.Println("Coordinater: Worker should not wait")
				}

				c.mu.Unlock()
				go c.checkCrash(i, "reduce")
				return nil
			}
		}
	} else {
		for i, t := range c.map_tasks {
			if t.Status == "available" {
				fmt.Println("Coordinator: Handing Map task ", i)
				c.map_tasks[i].Status = "in progress"
				reply.T = t
				reply.NReduce = c.nReduce
				reply.NMap = len(c.map_tasks)
				reply.Wait = false

				if reply.Wait == false {
					fmt.Println("Coordinater: Worker should not wait")
				}

				c.mu.Unlock()
				go c.checkCrash(i, "map")
				return nil
			}
		}
	}
	reply.Wait = true
	fmt.Println("Coordinator: Worker should wait")
	c.mu.Unlock()
	return nil

}

func (c *Coordinator) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	fmt.Println("Task ", args.Task_Number, " is being reported")
	if args.Success { // If the task was finished successfully
		if args.Task_Type == "map" {

			c.mu.Lock()
			fmt.Println("Coordinator: Receiving success of Map task ", args.Task_Number)
			c.map_tasks[args.Task_Number].Status = "done"
			c.mu.Unlock()

			if c.CheckMapDone() { // Check if all mapping tasks are done
				c.mu.Lock()
				c.mapping_done = true
				fmt.Println("All mapping tasks are done")
				c.mu.Unlock()
			}
		} else {
			fmt.Println("Coordinator: Receiving success of Reduce task ", args.Task_Number)

			c.mu.Lock()
			c.reduce_tasks[args.Task_Number].Status = "done"
			c.mu.Unlock()
		}
	} else { // If the worker failed to finish the task, we make the task available for other workers
		c.mu.Lock()
		if args.Task_Type == "map" {
			fmt.Println("Coordinator: Receiving failure of Map task ", args.Task_Number)
			c.map_tasks[args.Task_Number].Status = "available"
		} else {
			fmt.Println("Coordinator: Receiving failure of Reduce task ", args.Task_Number)
			c.reduce_tasks[args.Task_Number].Status = "available"
		}
		c.mu.Unlock()
	}
	reply.Received = true
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
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
	c.mu.Lock()
	if c.mapping_done {
		for _, t := range c.reduce_tasks {
			if t.Status != "done" {
				c.mu.Unlock()
				return false
			}
		}
	} else {
		c.mu.Unlock()
		return false
	}
	c.mu.Unlock()
	return true
}

//
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.nReduce = nReduce
	c.mapping_done = false
	for i, s := range files {
		c.map_tasks = append(c.map_tasks, Task{"map", i, "available", s})
		fmt.Println("Task Map no. ", i, "has been created")
	}
	for i := 0; i < c.nReduce; i++ {
		c.reduce_tasks = append(c.reduce_tasks, Task{"reduce", i, "available", ""})
	}

	c.server()

	return &c
}
