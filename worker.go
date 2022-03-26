package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

type ByKey []KeyValue

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {
		reply, not_done := CallGetTask()
		if not_done == false {
			break
		} else {
			if reply.T.Task_Type == "map" { // Given Task is Mapping
				fmt.Println("Got Map task ", reply.T.Task_Number)
				file, err := os.Open("./" + reply.T.Path)
				if err != nil {
					log.Fatalf("cannot open %v", reply.T.Path)
				}
				content, err := ioutil.ReadAll(file)
				if err != nil {
					log.Fatalf("cannot read %v", reply.T.Path)
				}
				file.Close()
				kva := mapf(reply.T.Path, string(content))

				// Open temp file and encoders
				intermediate_files := []*os.File{}
				encoders := []*json.Encoder{}
				for i := 0; i < reply.NReduce; i++ {
					intermediate_file, _ := ioutil.TempFile("", strconv.Itoa(i)+strconv.Itoa(reply.T.Task_Number))
					intermediate_files = append(intermediate_files, intermediate_file)
					enc := json.NewEncoder(intermediate_file)
					encoders = append(encoders, enc)
				}

				// Print the output in temporary files
				for i := 0; i < len(kva); i++ {
					hash := ihash(kva[i].Key) % reply.NReduce

					err := encoders[hash].Encode(&kva[i])
					if err != nil {
						GiveReport(false, reply.T.Task_Number, "map")
						log.Fatalf("cannot encode encoder %v", hash)
					}

				}

				// Renaming the temporary files
				for i := 0; i < reply.NReduce; i++ {
					intermediate_files[i].Close()
					dest := "./mr-" + strconv.Itoa(reply.T.Task_Number) + "-" + strconv.Itoa(i)
					os.Rename(intermediate_files[i].Name(), dest)

				}

				GiveReport(true, reply.T.Task_Number, "map")

			} else if reply.T.Task_Type == "reduce" {
				fmt.Println("Got Reduce task", reply.T.Task_Number)

				kva := []KeyValue{}

				// Reading the input files
				for i := 0; i < reply.NMap; i++ {
					dest := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(reply.T.Task_Number)
					file, err := os.Open("./" + dest)
					if err != nil {
						GiveReport(false, reply.T.Task_Number, "reduce")
						log.Fatalf("cannot open %v", dest)
					}
					dec := json.NewDecoder(file)
					for {
						var kv KeyValue
						if err := dec.Decode(&kv); err != nil {
							GiveReport(false, reply.T.Task_Number, "reduce")
							break
						}
						kva = append(kva, kv)
					}
				}

				// Performing the Reduce task
				sort.Sort(ByKey(kva))
				oname := "./mr-out" + strconv.Itoa(reply.T.Task_Number)
				ofile, _ := os.Create(oname)
				i := 0
				for i < len(kva) {
					j := i + 1
					for j < len(kva) && kva[j].Key == kva[i].Key {
						j++
					}
					values := []string{}
					for k := i; k < j; k++ {
						values = append(values, kva[k].Value)
					}
					output := reducef(kva[i].Key, values)

					// this is the correct format for each line of Reduce output.
					fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

					i = j
				}

				ofile.Close()
				GiveReport(true, reply.T.Task_Number, "reduce")

			}
		}
	}
}

func CallGetTask() (GetTaskReply, bool) {

	not_done := true

	for not_done {
		// declare an argument structure.
		args := GetTaskArgs{}

		// declare a reply structure.
		reply := GetTaskReply{}

		// send the RPC request, wait for the reply.
		not_done = call("Coordinator.GetTask", &args, &reply)
		if reply.Wait == false {
			fmt.Println("Worker: Received a task")
			return reply, not_done
		} else if not_done {
			time.Sleep(time.Second)
		}
	}
	return GetTaskReply{}, not_done
}

func GiveReport(success bool, Task_Number int, Task_Type string) {

	// declare an argument structure.
	args := ReportTaskArgs{success, Task_Number, Task_Type}

	// declare a reply structure.
	reply := ReportTaskReply{}

	fmt.Println("Finished", args.Task_Type, "task", args.Task_Number)

	// send the RPC request, wait for the reply.
	call("Coordinator.ReportTask", &args, &reply)

}



//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
