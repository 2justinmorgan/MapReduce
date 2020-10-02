package main

import (
	"fmt"
	"os"
	"bufio"
	"path/filepath"
	"hash/fnv"
	"plugin"
	"log"
)

func safeOpen(filepath string, option string) *os.File {
	var err error;
	var f *os.File;
	if option == "a" {
		f, err = os.OpenFile(filepath, os.O_APPEND|os.O_CREATE|os.O_WRONLY,0644);
	}
	if option == "r" {
		f, err = os.Open(filepath);
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "error opening file '%s'\n",filepath);
		os.Exit(1);
	}
	return f;
}

func safeWrite(filepath string, file *os.File, str string) {
	_, err := file.WriteString(str);
	if err != nil {
		fmt.Fprintf(os.Stderr, "error writing file '%s'\n",filepath);
		os.Exit(1);
	}
}

func getChunkFileName(fpath string, fnum int, M int) string {
	prefix := "input_files/chunks/%s_chunk_%03d_of_%03d.txt";
	chunkFileNum := fnum%M;
	if chunkFileNum == 0 { chunkFileNum = M; }
	chunkFileName := fmt.Sprintf(prefix, filepath.Base(fpath), chunkFileNum, M);
	return chunkFileName;
}

func checkDirExists(dirpath string) {
	if _, err := os.Stat(dirpath); os.IsNotExist(err) {
		os.Mkdir(dirpath, 0755)
	}
}

func hash(str string) int {
	hashVal := fnv.New32a();
	hashVal.Write([]byte(str));
	return int(hashVal.Sum32());
}

func createChunkFiles(filepath string) map[string]*os.File {
	checkDirExists("input_files/chunks/");
	lineNum := 0;
	file := safeOpen(filepath, "r");
	scanner := bufio.NewScanner(file);

	chunkFiles := make(map[string]*os.File);
	for i:=1; i<=M; i++ {
		chunkFileName := getChunkFileName(filepath, i, M);
		os.Remove(chunkFileName);
		chunkFiles[chunkFileName] = safeOpen(chunkFileName, "a");
	}

	for scanner.Scan() {
		lineNum++;
		chunkFileName := getChunkFileName(filepath, lineNum, M);
		chunkFile := chunkFiles[chunkFileName];
		safeWrite(filepath, chunkFile, scanner.Text()+"\n");
	}

	for _, file := range chunkFiles {
		file.Close();
	}

	file.Close();
	return chunkFiles
}

func loadPlugin(filename string) (func(string, string) []string, func(string, []string) string) {
	p, err := plugin.Open(filename)
	if err != nil {
		log.Fatalf("cannot load plugin %v", filename)
	}
	xmapf, err := p.Lookup("Map")
	if err != nil {
		log.Fatalf("cannot find Map in %v", filename)
	}
	mapf := xmapf.(func(string, string) []string)
	xreducef, err := p.Lookup("Reduce")
	if err != nil {
		log.Fatalf("cannot find Reduce in %v", filename)
	}
	reducef := xreducef.(func(string, []string) string)

	return mapf, reducef
}
