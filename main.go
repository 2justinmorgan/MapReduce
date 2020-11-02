package main

import (
	"fmt"
	"os"
	"math"
)

const numWorkers = 8
//number of bytes in each inputFile-chunk
const chunkSize = 100
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

func getNumMapTasks(filePath string) int {
	file := safeOpen(filePath, "r")
	fileInfo, fileStatErr := file.Stat()
	if fileStatErr != nil {
		fmt.Fprintf(os.Stderr, "error stat on file '%s'\n",filePath);
		os.Exit(1);
	}
	fileSize := fileInfo.Size()
	numMapTasks := float64(fileSize) / float64(chunkSize)
	return int(math.Ceil(numMapTasks))
}

func main() {
	inputFilePath, soFilepath := checkArgs(len(os.Args), os.Args)

	numMapTasks := getNumMapTasks(inputFilePath)

	fmt.Println("exiting early (input file chunking is being re-implemented)")
	os.Exit(0)

	// to be re-implemented
	chunkFiles := createChunkFiles(inputFilePath, numMapTasks)


	createOutputDirs([]string {"./intermediate_files","./output_files"})
	launchWorkers(soFilepath, chunkFiles, numMapTasks)
}





