package main

import (
	"fmt"
	"os"
	"./mr"
)

const numWorkers = 8
//number of chunks of input data
const M = 8
//number of reduce tasks
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

	createOutputDirs([]string {"./intermediate_files","./output_files"})
	launchWorkers(sofilepath, chunkFiles)
}

func buildWorkers(numWorkers int) []*Worker {
	workers := make([]*Worker, numWorkers)

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
	return workers
}

func buildMapTasks(
	M int,
	chunkFiles map[string]*os.File,
	mapf (func(string,string) []mr.KeyVal)) []*MapTask {

	mapTasks := make([]*MapTask, M)

	chunkFileNames := make([]string, len(chunkFiles))
	i := 0;
	for k := range chunkFiles { 
		chunkFileNames[i] = k;
		i++;
	}

	for i := 0; i < M; i++ {
		mapTasks[i] = &MapTask{
			id:		i,
			mapf:		mapf,
			chunk:	chunkFiles[chunkFileNames[i]],
		}
	}
	return mapTasks
}

func buildReduceTasks(
	R int,
	reducef (func(string,[]string) string)) []*ReduceTask {

	reduceTasks := make([]*ReduceTask, R)

	for i := 0; i < R; i++ {
		reduceTasks[i] = &ReduceTask{
			id:		i,
			reducef:	reducef,
		}
	}
	return reduceTasks
}

func build(sofilepath string, chunkFiles map[string]*os.File) ([]*Worker, []*MapTask, []*ReduceTask) {
	mapf, reducef := loadPlugin(sofilepath)
	workers := buildWorkers(numWorkers)
	mapTasks := buildMapTasks(M, chunkFiles, mapf)
	reduceTasks := buildReduceTasks(R, reducef)

	return workers, mapTasks, reduceTasks
}




