package main

import (
	"time"
	"fmt"
	"math/rand"
	"sync"
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
}

type TableEntry struct {
	id 	int
	hb 	int
	t  	int
	mux 	sync.RWMutex
}

type Task struct {
	//TODO
}


func (worker *Worker) runMaster() {
	go worker.updateHB()
	go worker.gossip()
	//read work requests from workers and assign work to them
	for {
		for len(worker.workRequests) > 0 {
			requestID := <- worker.workRequests
			go worker.assignWork(requestID, Task{}) //TODO get task
		}
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
		fmt.Printf("work completed signal read in worker %d\n", worker.id)
	}
}

func (worker *Worker) assignWork(requestID int, work Task) {
	go worker.workers[requestID].doWork(work)
	//wait for work completed signal
	for len(worker.workers[requestID].workCompleted) == 0 {
		//node failure detected
		worker.table[requestID].mux.Lock()
		hb := worker.table[requestID].hb
		worker.table[requestID].mux.Unlock()
		if hb != -1 {
			//TODO reassign task to a different worker
			break
		}
	}
}

func (worker *Worker) doWork(work Task) {
	//TODO do mapping and reducing
	worker.workCompleted <- 1
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