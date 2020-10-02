package main

import (
	"fmt"
	"os"
)


const numWorkers = 8
//number of chunks of input data, this should be computed based on the file size
const M = 8
//number of reduce tasks, we can also have this be a command line arg
const R = 8

func checkArgs(argc int, argv []string) (string, string) {
	if argc != 3 {
		fmt.Fprintf(os.Stderr, "Usage: ./main <input_file.txt> <*.so>\n")
		os.Exit(1)
	}
	if _, err := os.Stat(argv[1]); os.IsNotExist(err) {
		fmt.Fprintf(os.Stderr, "'%s' not in current directory\n",argv[1])
		os.Exit(1)
	}
	return argv[1], argv[2]
}

func main() {
	filename, sofilepath := checkArgs(len(os.Args), os.Args)
	chunkFiles := createChunkFiles(filename)

	workers, mapTasks, reduceTaks := build(sofilepath, chunkFiles)	
	//master is worker with id 0
	go workers[0].runMaster(mapTasks, reduceTaks)
	//launch the rest of the workers
	for i := 1; i < numWorkers; i++ {
		go workers[i].run()
	}
	for len(workers[0].workCompleted) < M+R{
		//let workers run
	}
	fmt.Printf("finished\n")
}

func build(sofilepath string, chunkFiles map[string]*os.File) ([]*Worker, []*MapTask, []*ReduceTask) {
	workers := make([]*Worker, numWorkers)
	mapTasks := make([]*MapTask, M)
	reduceTasks := make([]*ReduceTask, R)
	mapf, reducef := loadPlugin(sofilepath)

	chunkFileNames := make([]string, len(chunkFiles))
	i := 0;
	for k := range chunkFiles { 
		chunkFileNames[i] = k;
		i++;
	}

	for i := 0; i < numWorkers; i++ {
		workers[i] = &Worker{
			id:       		i,
			workers:  		workers,
			table:			make([]*TableEntry, numWorkers),
			tableInput:    make(chan []*TableEntry, numWorkers*2),
			workRequests:  make(chan int, M*R),
			workCompleted: make(chan int, M*R),
			redoMap:			make(chan *MapTask, M),
			redoReduce:		make(chan *ReduceTask, R),
		}
		for j := 0; j < numWorkers; j++ {
			workers[i].table[j] = &TableEntry{id: j, hb: 0, t: 0}
		}
	}
	for i := 0; i < M; i++ {
		mapTasks[i] = &MapTask{
			id:		i,
			mapf:		mapf,
			chunk:	chunkFiles[chunkFileNames[i]],
		}
	}
	for i := 0; i < R; i++ {
		reduceTasks[i] = &ReduceTask{
			id:		i,
			reducef:	reducef,
		}
	}
	return workers, mapTasks, reduceTasks
}




