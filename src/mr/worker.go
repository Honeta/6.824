package mr

import (
	"bufio"
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

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	filename := ""
	fileno := -1
	nreduce := 0
	eof := false
	for !eof {
		filename, fileno, nreduce, eof = CallAllocMap(fileno)
		if fileno == -1 {
			time.Sleep(time.Second)
			continue
		}
		contents, err := os.ReadFile(filename)
		if err != nil {
			panic("readfile " + filename + " failed!")
		}
		kv := mapf(filename, string(contents))
		sort.Slice(kv, func(i, j int) bool {
			if ihash(kv[i].Key)%nreduce == ihash(kv[j].Key)%nreduce {
				return kv[i].Key < kv[j].Key
			} else {
				return ihash(kv[i].Key)%nreduce < ihash(kv[j].Key)%nreduce
			}
		})
		var file_name string
		var file *os.File
		for i := 0; i < len(kv); i++ {
			if i == 0 || ihash(kv[i].Key)%nreduce != ihash(kv[i-1].Key)%nreduce {
				if i != 0 {
					file.Close()
					err := os.Rename(file.Name(), file_name)
					if err != nil {
						panic(err.Error() + " - rename " + file.Name() + " failed!\n")
					}
				}
				file_name = "mr-" + strconv.Itoa(fileno) + "-" + strconv.Itoa(ihash(kv[i].Key)%nreduce)
				file, err = ioutil.TempFile(".", file_name+"-")
				if err != nil {
					panic(err.Error() + " - openfile " + file.Name() + " failed!\n")
				}
			}
			fmt.Fprintf(file, "%v %v\n", kv[i].Key, kv[i].Value)
		}
		file.Close()
		err = os.Rename(file.Name(), file_name)
		if err != nil {
			panic(err.Error() + " - rename " + file.Name() + " failed!\n")
		}
	}

	reduceno := -1
	reduced := false
	filesize := 0
	for !reduced {
		reduceno, reduced, filesize = CallAllocReduce(reduceno)
		if reduceno == -1 {
			time.Sleep(time.Second)
			continue
		}
		var kv []KeyValue
		for i := 0; i < filesize; i++ {
			filename := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(reduceno)
			file, err := os.OpenFile(filename, os.O_RDONLY, 0)
			if err != nil {
				continue
			}
			sc := bufio.NewScanner(file)
			for sc.Scan() {
				str := sc.Text()
				i := 0
				for ; str[i] != ' '; i++ {
				}
				kv = append(kv, KeyValue{str[:i], str[i+1:]})
			}
		}
		if len(kv) == 0 {
			continue
		}
		sort.Slice(kv, func(i, j int) bool {
			return kv[i].Key < kv[j].Key
		})
		var value []string
		var result []KeyValue
		for i := 0; i < len(kv); i++ {
			if i > 0 && kv[i].Key != kv[i-1].Key {
				tmp := reducef(kv[i-1].Key, value)
				result = append(result, KeyValue{kv[i-1].Key, tmp})
				value = []string{}
			}
			value = append(value, kv[i].Value)
		}
		tmp := reducef(kv[len(kv)-1].Key, value)
		result = append(result, KeyValue{kv[len(kv)-1].Key, tmp})
		filename := "mr-out-" + strconv.Itoa(reduceno)
		file, err := ioutil.TempFile(".", filename+"-")
		if err != nil {
			panic(err.Error() + " - openfile " + file.Name() + " failed!\n")
		}
		for i := 0; i < len(result); i++ {
			fmt.Fprintf(file, "%v %v\n", result[i].Key, result[i].Value)
		}
		file.Close()
		err = os.Rename(file.Name(), filename)
		if err != nil {
			panic(err.Error() + " - rename " + file.Name() + " failed!\n")
		}
	}
	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func CallAllocMap(fileno int) (string, int, int, bool) {
	args := AllocMapArgs{}
	args.Fileno = fileno
	reply := AllocMapReply{}
	ok := call("Coordinator.AllocMap", &args, &reply)
	if ok {
		return reply.Filename, reply.Fileno, reply.Nreduce, reply.Eof
	} else {
		panic("call allocmap failed!\n")
	}
}

func CallAllocReduce(reduceno int) (int, bool, int) {
	args := AllocReduceArgs{}
	args.Reduceno = reduceno
	reply := AllocReduceReply{}
	ok := call("Coordinator.AllocReduce", &args, &reply)
	if ok {
		return reply.Reduceno, reply.Reduced, reply.Filesize
	} else {
		panic("call allocreduce failed!")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
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
