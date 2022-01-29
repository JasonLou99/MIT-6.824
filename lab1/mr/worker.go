package mr

import (
	"bufio"
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

const (
	FAIL    = 1
	SUCCESS = 0
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
	// New32a 返回一个新的32位 FNV-1a hash.Hash 。它的 Sum 方法将以 big-endian 字节顺序排列值。
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// for sorting by key.
// kv切片类型的别名：ByKey
type ByKey []KeyValue

// for sorting by key.
// 切片的API
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	over := 0
	for over == 0 {
		args := Args{}
		reply := Reply{}
		call("Coordinator.AskTaskHandler", &args, &reply)
		switch reply.TaskTypes {
		case "map":
			fmt.Println("worker running map")
			// 执行map任务
			// 读取文件内容 进行map
			filename := reply.TaskFile
			mapTaskCount := reply.TaskId
			mapRes := doMap(filename, mapTaskCount, mapf)
			args = Args{}
			args.TaskResult = mapRes
			args.MapOrReduce = "map"
			args.TaskId = reply.TaskId
			// 发送运行结果
			call("Coordinator.SubmitTaskHandler", &args, &reply)
		case "reduce":
			fmt.Println("worker running reduce")
			// 执行reduce任务
			reduceNumber := reply.TaskId - 8
			reduceRes := doReduce(reduceNumber, reducef)
			args = Args{}
			args.TaskResult = reduceRes
			args.MapOrReduce = "reduce"
			args.TaskId = reply.TaskId
			call("Coordinator.SubmitTaskHandler", &args, &reply)
		case "wait":
			time.Sleep(time.Second)
		case "exit":
			// 退出循环，worker结束
			// fmt.Println("Worker exit")
			over = 1
		case "":
			// fmt.Println("can't send RPC to Coordinator , Worker exit")
			over = 1
		}

	}

}

// 请求任务
// func askTask(args interface{}, reply interface{}) {
// 	// 获取文件

// 	call("Coordinator.AskTaskHandler", &args, &reply)

// }

// 执行map task
func doMap(filename string, mapTaskCount int, mapf func(string, string) []KeyValue) int {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
		return FAIL
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
		return FAIL
	}
	file.Close()
	kva := mapf(filename, string(content))
	sort.Sort(ByKey(kva))

	// 结果写入中间文件iFile
	i := 0
	for i < len(kva) {
		key := kva[i].Key
		y := ihash(key) % 10

		iName := "mr-" + strconv.Itoa(mapTaskCount) + "-" + strconv.Itoa(y)
		// 创建文件写入内容或者直接写入内容：key 1
		ifile, err := os.OpenFile(iName, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0777)
		if err != nil {
			log.Fatalf("cannot append/create %v, key: %v", iName, kva[i])
			return FAIL
		}
		write := bufio.NewWriter(ifile)
		// 仿json.NewEncoder的写入，方便读取
		// {"Key":"lou","Value":"1"}
		str := "{\"Key\":\"" + kva[i].Key + "\",\"Value\":\"" + kva[i].Value + "\"}\n"
		write.WriteString(str)
		//Flush将缓存的文件真正写入到文件中
		write.Flush()
		ifile.Close()
		i++
	}
	// kva写完说明一个map任务完成，这会产生多个中间文件，内容都是：key 1
	return SUCCESS
}

// 执行reduce task
func doReduce(reduceNumber int, reducef func(string, []string) string) int {
	var kva []KeyValue
	// 读取所有文章的对应map结果到kva中
	for i := 0; i < 8; i++ {
		x := strconv.Itoa(i)
		// 读取一篇文章的对应的map结果到kva中
		y := strconv.Itoa(reduceNumber)
		rFilename := "mr-" + x + "-" + y
		rFile, _ := os.Open(rFilename)
		// if err != nil {
		// 	log.Fatalf("cannot open %v", rFilename)
		// 	return FAIL
		// }

		dec := json.NewDecoder(rFile)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		rFile.Close()
	}
	sort.Sort(ByKey(kva))
	oname := "mr-out-" + strconv.Itoa(reduceNumber)
	ofile, _ := os.Create(oname)
	i := 0
	// 统计并写入到最终的文件中
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
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)
		i = j
	}
	ofile.Close()
	return SUCCESS
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
