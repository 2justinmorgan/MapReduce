package main


const numWorkers = 8

func main() {
	workers := buildWorkers()	
	//master is worker with id 0
	go workers[0].runMaster()
	//launch the rest of the workers
	for i := 1; i < numWorkers; i++ {
		go workers[i].run()
	}
	for {
		//let workers run
	}
}

func buildWorkers() ([]*Worker) {
	workers := make([]*Worker, numWorkers)
	for i := 0; i < numWorkers; i++ {
		workers[i] = &Worker{
			id:       		i,
			workers:  		workers,
			table:			make([]*TableEntry, numWorkers),
			tableInput:    make(chan []*TableEntry, 10),
			workRequests:  make(chan int, 10),
			workCompleted: make(chan int, 10),
		}
		for j := 0; j < numWorkers; j++ {
			workers[i].table[j] = &TableEntry{id: j, hb: 0, t: 0}
		}
	}
	return workers
}




