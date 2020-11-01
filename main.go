package main

import (
	"fmt"
	"os"
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





