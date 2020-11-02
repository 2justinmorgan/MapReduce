package main

import (
	"fmt"
	"os"
	"bufio"
	"path/filepath"
	"hash/fnv"
	"plugin"
	"log"
	"io/ioutil"
	"./mr"
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
		fmt.Println(err)
		os.Exit(1);
	}
	return f;
}

func safeRead(filepath string) string {
	fileContentBytes, readErr := ioutil.ReadFile(filepath);
	fileContent := string(fileContentBytes);
	if readErr != nil {
		fmt.Fprintf(os.Stderr, "error reading file '%s'\nmsg:\n%s",
			filepath, readErr);
		os.Exit(1);
	}
	return fileContent;
}

func safeWrite(filepath string, content string) {
   err := ioutil.WriteFile(filepath, []byte(content), 0644)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error writing file '%s'\nmsg:\n%s\n",
			filepath, err);
		os.Exit(1);
   }
}

func safeAppend(filepath string, content string) {
	f, err := os.OpenFile(filepath,os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error opening file '%s'\nmsg:\n%s\n",
			filepath, err);
		os.Exit(1);
	}
	defer f.Close();
	if _, err := f.WriteString(content); err != nil {
		fmt.Fprintf(os.Stderr, "error appending file '%s'\nmsg:\n%s\n",
			filepath, err);
		os.Exit(1);
	}
}

func getChunkFileName(fpath string, workerNum int, M int) string {
	prefix := "input_files/chunks/%s_chunk_%03d_of_%03d.txt";
	chunkFileNum := workerNum % M;
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

func createChunkFiles(filepath string, numMapTasks int) map[string]*os.File {
	checkDirExists("input_files/chunks/");
	lineNum := 0;
	file := safeOpen(filepath, "r");
	scanner := bufio.NewScanner(file);

	chunkFiles := make(map[string]*os.File);
	for i:=1; i<=numMapTasks; i++ {
		chunkFileName := getChunkFileName(filepath, i, numMapTasks);
		os.Remove(chunkFileName);
		chunkFiles[chunkFileName] = safeOpen(chunkFileName, "a");
	}

	for scanner.Scan() {
		lineNum++;
		chunkFileName := getChunkFileName(filepath, lineNum, numMapTasks);
		safeAppend(chunkFileName, scanner.Text()+"\n");
	}

	for _, file := range chunkFiles {
		file.Close();
	}

	file.Close();
	return chunkFiles
}

func loadPlugin(filename string) (func(string, string) []mr.KeyVal, func(string, []string) string) {
	p, err := plugin.Open(filename)
	if err != nil {
		log.Fatalf("cannot load plugin %v", filename)
	}
	xmapf, err := p.Lookup("Map")
	if err != nil {
		log.Fatalf("cannot find Map in %v", filename)
	}
	mapf := xmapf.(func(string, string) []mr.KeyVal)
	xreducef, err := p.Lookup("Reduce")
	if err != nil {
		log.Fatalf("cannot find Reduce in %v", filename)
	}
	reducef := xreducef.(func(string, []string) string)

	return mapf, reducef
}

func createOutputDirs(dirs []string) {
	//make dir for intermediate files and output files to go in
	path := "./intermediate_files"
	if _, err := os.Stat(path); os.IsNotExist(err) {
    	os.Mkdir(path, 0700)
	}
	path = "./output_files"
	if _, err := os.Stat(path); os.IsNotExist(err) {
    	os.Mkdir(path, 0700)
	}
}

func readFileByByteRange(start int64, offset int64, filePath string) string {
	file := safeOpen(filePath, "r")
	// advance file head 'start' number of bytes
	val, seekErr := file.Seek(start, 0)
	_ = val
	if seekErr != nil {
		fmt.Fprintf(os.Stderr, "error file seek '%s'\n",file.Name());
		os.Exit(1);
	}

	// read 'offset' number of bytes from file
	content := make([]byte, offset)
	nBytesRead, readErr := file.Read(content)
	_ = nBytesRead
	if readErr != nil {
		fmt.Fprintf(os.Stderr, "error file read '%s'\n",file.Name());
		os.Exit(1);
	}

	return string(content)
}






