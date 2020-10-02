package main

import (
	"time"
	"fmt"
	"math/rand"
	"sync"
	"os"
	"./mr"
	"io/ioutil"
	"encoding/json"
	"sort"
)

//seconds to wait before updating heartbeat
const X = 1
//seconds to wait before gossiping table
const Y = 1
//seconds to wait before declaring a node failed
const t_fail = 4

type Worker struct {
	id				  int
	workers       []*Worker
	table         []*TableEntry
	tableInput    chan []*TableEntry
	workRequests  chan int
	workCompleted chan int
	redoMap		  chan *MapTask
	redoReduce    chan *ReduceTask
}

type TableEntry struct {
	id 	int
	hb 	int
	t  	int
	mux 	sync.RWMutex
}

type MapTask struct {
	id 	int
	mapf (func(string, string) []mr.KeyVal)
	chunk *os.File
}

type ReduceTask struct {
	id 	int
	reducef (func(string, []string) string)
}

func (worker *Worker) runMaster(mapTasks []*MapTask, reduceTasks []*ReduceTask) {
	go worker.updateHB()
	go worker.gossip()

	//read work requests from workers and assign work to them
	//first run all map tasks
	for len(worker.workCompleted) < M {
		//add uncompleted tasks back to list
		if len(worker.redoMap) > 0 {
			task := <- worker.redoMap
			mapTasks = append(mapTasks, task)
		}
		if len(mapTasks) == 0 {
			//wait for tasks to complete
			continue
		}
		requestID := <- worker.workRequests
		//pop first task
		task := mapTasks[0]
		mapTasks = mapTasks[1:]
		go worker.assignMap(requestID, task)
	}
	//run all reduce tasks
	for len(worker.workCompleted) < M+R {
		//add uncompleted tasks back to list
		if len(worker.redoReduce) > 0 {
			task := <- worker.redoReduce
			reduceTasks = append(reduceTasks, task)
		}
		if len(reduceTasks) == 0 {
			//wait for tasks to complete
			continue
		}
		requestID := <- worker.workRequests
		//pop first task
		task := reduceTasks[0]
		reduceTasks = reduceTasks[1:]
		go worker.assignReduce(requestID, task)
	}
}


func (worker *Worker) run() {
	go worker.updateHB()
	go worker.gossip()
	for {
		//send this workers ID to the master to request a task
		worker.workers[0].workRequests <- worker.id
		for len(worker.workCompleted) == 0 {
			//wait for work to finish
		}
		<- worker.workCompleted
	}
}

func (worker *Worker) assignMap(requestID int, task *MapTask) {
	go worker.workers[requestID].doMap(task)
	//wait for work completed signal
	for len(worker.workers[requestID].workCompleted) == 0 {
		//node failure detected
		worker.table[requestID].mux.Lock()
		hb := worker.table[requestID].hb
		worker.table[requestID].mux.Unlock()
		if hb == -1 {
			//send uncompleted task back to master
			worker.workers[0].redoMap <- task
			break
		}
	}
}

func (worker *Worker) assignReduce(requestID int, task *ReduceTask) {
	go worker.workers[requestID].doReduce(task)
	//wait for work completed signal
	for len(worker.workers[requestID].workCompleted) == 0 {
		//node failure detected
		worker.table[requestID].mux.Lock()
		hb := worker.table[requestID].hb
		worker.table[requestID].mux.Unlock()
		if hb == -1 {
			//send uncompleted task back to master
			worker.workers[0].redoReduce <- task
			break
		}
	}
}

func (worker *Worker) doMap(task *MapTask) {
	chunkFileContent := safeRead(task.chunk.Name())
	kva := task.mapf(task.chunk.Name(),chunkFileContent)
	m := make(map[int][]mr.KeyVal, R)
	for _, kv := range kva {
		partitionNum := hash(kv.Key) % R
		m[partitionNum] = append(m[partitionNum], kv) 
	}
	files := make([]*os.File, R)
	for i := 0; i < R; i++ {
		tempname := fmt.Sprintf("temp-mr-%03d-%03d", task.id, i)
		files[i], _ = ioutil.TempFile("./intermediate_files", tempname)
		enc := json.NewEncoder(files[i])
		for _, kv := range m[i] {
			enc.Encode(&kv)
		}
	}
	//rename files once work is completed
	for i := 0; i < R; i++ {
		filename := fmt.Sprintf("./intermediate_files/mr-%03d-%03d", task.id, i)
		os.Rename(files[i].Name(), filename)
		os.Remove(files[i].Name())
	}
	worker.workCompleted <- 1
	worker.workers[0].workCompleted <- 1
}

func (worker *Worker) doReduce(task *ReduceTask) {
	
	oname := fmt.Sprintf("./output_files/mr-out-%03d", task.id)
	ofile, _ := os.Create(oname)

	for i := 0; i < M; i++ {
		kva := []mr.KeyVal{}
		filename := fmt.Sprintf("./intermediate_files/mr-%03d-%03d", i, task.id)
		file := safeOpen(filename, "r")
		dec := json.NewDecoder(file)
		for {
			var kv mr.KeyVal
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		sort.Slice(kva, func(i, j int) bool {
  			return kva[i].Key < kva[j].Key
		})

		i := 0
		for i < len(kva) {
			j := i + 1
			for j < len(kva) && kva[j].Key == kva[i].Key {
				j++
			}
			values := []string{}
			for k := i; k < j; k++ {
				values = append(values, kva[k].Key)
			}
			output := task.reducef(kva[i].Key, values)
			fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)
			i = j
		}
	}
	ofile.Close()
	worker.workCompleted <- 1
	worker.workers[0].workCompleted <- 1
	
}

//update workers HB and clock periodically
func (worker *Worker) updateHB() {
	for{
		time.Sleep(X * time.Second)
		worker.table[worker.id].mux.Lock()
		worker.table[worker.id].hb++
		worker.table[worker.id].t += X
		worker.table[worker.id].mux.Unlock()
	}	
}

//periodically gossip with two random neighbors and check for failures
func (worker *Worker) gossip() {
	for{
		time.Sleep(Y * time.Second)
		//send table to neighbors
		neighbors := getRandNeighbors(worker.id)
		worker.workers[neighbors[0]].tableInput <- worker.table
		worker.workers[neighbors[1]].tableInput <- worker.table
		//read tables sent from neighbors and update
		for len(worker.tableInput) > 0 {
			neighborTable := <- worker.tableInput
			worker.table[worker.id].mux.RLock()
			currTime := worker.table[worker.id].t 
			worker.table[worker.id].mux.RUnlock()
			for i := 0; i < numWorkers; i++ {
				neighborTable[i].mux.RLock()
				tNeighbor := neighborTable[i].t
				hbNeighbor := neighborTable[i].hb
				neighborTable[i].mux.RUnlock()
				worker.table[i].mux.Lock()
				if tNeighbor >= worker.table[i].t && hbNeighbor > worker.table[i].hb && worker.table[i].hb != -1 {
					worker.table[i].t = currTime
					worker.table[i].hb = hbNeighbor
				}
				worker.table[i].mux.Unlock()
			}
		}
		//check for failures
		for i := 0; i < numWorkers; i++ {
			worker.table[worker.id].mux.RLock()
			currTime := worker.table[worker.id].t 
			worker.table[worker.id].mux.RUnlock()
			worker.table[i].mux.Lock()
			if currTime - worker.table[i].t > t_fail && worker.table[i].hb != -1 && worker.table[i].t != 0 {
				worker.table[i].hb = -1	
			}
			worker.table[i].mux.Unlock()
		}
	}
}

//pick 2 unique random neighbors
func getRandNeighbors(id int) [2]int {
	rand.Seed(time.Now().UnixNano())
	neighbors := [2]int{}
	rand1 := rand.Intn(numWorkers)
	for rand1 == id {
		rand1 = rand.Intn(numWorkers)
	}
	neighbors[0] = rand1
	rand2 := rand.Intn(numWorkers)
	for rand2 == rand1 || rand2 == id {
		rand2 = rand.Intn(numWorkers)
	}
	neighbors[1] = rand2
	return neighbors
}

//print table for worker in format [id|hb|time]
func (worker *Worker) printTable() {
	fmt.Printf("node %v table: ", worker.id)
	for i := 0; i < numWorkers; i++ {	
		worker.table[i].mux.RLock()
		fmt.Printf("[%v|%2v|%2v]   ", worker.table[i].id, worker.table[i].hb, worker.table[i].t)
		worker.table[i].mux.RUnlock()
	}
	fmt.Printf("\n")
}

