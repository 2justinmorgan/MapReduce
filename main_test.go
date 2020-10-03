package main

import (
	"fmt"
	"testing"
	"io/ioutil"
)

func TestGetChunkFileName(t *testing.T) {
	var testCases = []struct {
		fpath string
		workerNum, M int
		expect string
	}{
		{
			"./this/one.txt",
			7,
			8,
			"input_files/chunks/one.txt_chunk_007_of_008.txt",
		},
		{
			"some.file",
			35,
			6,
			"input_files/chunks/some.file_chunk_005_of_006.txt",
		},
		{
			"./yes_file",
			105,
			200,
			"input_files/chunks/yes_file_chunk_105_of_200.txt",
		},
		{
			"./dir/500/t.txt",
			0,
			1,
			"input_files/chunks/t.txt_chunk_001_of_001.txt",
		},
		{
			"./dir/somethin/f.txt",
			1030,
			540,
			"input_files/chunks/f.txt_chunk_490_of_540.txt",
		},
	}

	for i, testCase := range testCases {
		testName := fmt.Sprintf("test%d %s...",i,testCase.fpath[0:5]);
		t.Run(testName, func(t *testing.T) {
			actual := getChunkFileName(
				testCase.fpath, testCase.workerNum, testCase.M);
			if actual != testCase.expect {
				t.Errorf("%s != %s", actual, testCase.expect);
			}
		});
	}
}

func TestSafeWrite(t *testing.T) {
	var testCases = []struct {
		fpath string
		inputContent string
	}{
		{
			"test_files/TestFwriteForTesting/testFwrite1",
			"this\nis\nthe\tfirst  \ntest\n",
		},
		{
			"test_files/TestFwriteForTesting/testFwrite2",
			"\n\nanother\t 445   \ttest",
		},
	}

	for i, testCase := range testCases {
		testName := fmt.Sprintf("test%d %s",i,testCase.inputContent);
		t.Run(testName, func(t *testing.T) {
			safeWrite(testCase.fpath, testCase.inputContent);
			fileContentBytes, readErr := ioutil.ReadFile(testCase.fpath);
			fileContent := string(fileContentBytes);
			if readErr != nil {
				t.Errorf("error reading '%s'\nmsg:\n%s", testCase.fpath, readErr);
			} else if fileContent != testCase.inputContent {
				t.Errorf("%s != %s", fileContent, testCase.inputContent);
			}
		});
	}
}

func TestSafeAppend(t *testing.T) {
	var testCases = []struct {
		filepath string
		preExistingFileContent string
		fileContentToAppend string
	}{
		{
			"test_files/TestFwriteForTesting/testFappend1",
			"this is file content that exists prior to testing\n",
			"here is some file content that should be appended\n",
		},
		{
			"test_files/TestFwriteForTesting/testFappend2",
			" \t  \n \t\t  some strange\n\t formatting 89829 *9` \n",
			"  \n more that \t \n should be appended \t\n",
		},
	}

	for i, testCase := range testCases {
		testName := fmt.Sprintf("test%d %s",i,testCase.preExistingFileContent);
		t.Run(testName, func(t *testing.T) {
			safeWrite(testCase.filepath, testCase.preExistingFileContent);
			safeAppend(testCase.filepath, testCase.fileContentToAppend);
			expectedFinalFileContent := 
				testCase.preExistingFileContent + testCase.fileContentToAppend;
			appendedFinalFileContent := safeRead(testCase.filepath);
			if appendedFinalFileContent != expectedFinalFileContent {
				t.Errorf("\n%s != \n%s", 
					appendedFinalFileContent, expectedFinalFileContent);
			}
		});
	}
}

func TestSafeRead(t *testing.T) {
	var testCases = []struct {
		inputFileContent string
		filepath string
	}{
		{
			"this is some file content",
			"test_files/TestSafeRead/testSR01",
		},
		{
			"\n\t here is another\n  \n \t   test case\n",
			"test_files/TestSafeRead/testSR02",
		},
	}

	for i, testCase := range testCases {
		testName := fmt.Sprintf("test%d %s...",i,testCase.inputFileContent[0:5]);
		t.Run(testName, func(t *testing.T) {
			safeWrite(testCase.filepath, testCase.inputFileContent);
			actualFileContent := safeRead(testCase.filepath);
			if actualFileContent != testCase.inputFileContent {
				t.Errorf("%s != %s", actualFileContent, testCase.inputFileContent);
			}
		});
	}
}

func TestHash(t *testing.T) {
	var testCases = []struct {
		inputString string
		expectHashValue int
	}{
		{"wordX",1941425855},
		{"spacing in the input string",982984679},
		{"random-word-5000:",1166272117},
		{"",2166136261},
	}

	for i, testCase := range testCases {
		testName := fmt.Sprintf("test%d %s",i,testCase.inputString);
		t.Run(testName, func(t *testing.T) {
			actualHashValue := hash(testCase.inputString);
			if  actualHashValue!= testCase.expectHashValue {
				t.Errorf("%d != %d", actualHashValue, testCase.expectHashValue);
			}
		});
	}
}

// TODO need to update test for doMap() instead of mapAndPartition()
//
//func TestMapAndPartition(t *testing.T) {
//	var testCases = []struct {
//		chunkFilePath string
//		chunkFileContent string
//		numOfPartitions int
//		partitionFilePaths []string
//		partitionFilesContents []string
//	}{
//		{
//			"test_files/TestMapAndPartition/testMAP01_inputFile",
//			"here are a bunch of words that will be tokenized and " +
//			"written to partition files",
//			3,
//			[]string{
//				"test_files/TestMapAndPartition/testMAP01_partition001",
//				"test_files/TestMapAndPartition/testMAP01_partition002",
//				"test_files/TestMapAndPartition/testMAP01_partition003",
//			},
//			[]string{
//				"here\nbunch\nof\nwill\nbe\nto\nfiles\n",
//				"are\na\nwords\nthat\ntokenized\nwritten\npartition\n",
//				"and\n",
//			},
//		},
//		{
//			"test_files/TestMapAndPartition/testMAP02_inputFile",
//			"\n\tSample Title 01:\n\tOnce upon a time, there was " +
//			"a test case that\nalmost broke the code. But a final " +
//			"result of the implementation was as follows:\n  \n \t " +
//			"* so many things happen\n*not enough times\n* ...random",
//			8,
//			[]string{
//				"test_files/TestMapAndPartition/testMAP02_partition001",
//				"test_files/TestMapAndPartition/testMAP02_partition002",
//				"test_files/TestMapAndPartition/testMAP02_partition003",
//				"test_files/TestMapAndPartition/testMAP02_partition004",
//				"test_files/TestMapAndPartition/testMAP02_partition005",
//				"test_files/TestMapAndPartition/testMAP02_partition006",
//				"test_files/TestMapAndPartition/testMAP02_partition007",
//				"test_files/TestMapAndPartition/testMAP02_partition008",
//			},
//			[]string{
//				"01:\ntime\nof\n",
//				"Title\nupon\ncase\nfollows\nhash\nhappen\nenough\n",
//				"was\nwas\nmany\n*not\n",
//				"",
//				"Once\na\na\nthat\nthe\nBut\na\nresult\nthe\nthings\n",
//				"there\ntest\nas\ntimes\n",
//				"broke\ncode.\n\n...random\n",
//				"Sample\nalmost\nfinal\nimplementation\n",
//			},
//		},
//	}
//
//	for i, testCase := range testCases {
//		testName := fmt.Sprintf("test%d %s",i,testCase.chunkFileContent[0:10]);
//		t.Run(testName, func(t *testing.T) {
//
//			// make chunk file
//			os.Remove(testCase.chunkFilePath);
//			safeWrite(testCase.chunkFilePath, testCase.chunkFileContent);
//			mapAndPartition(testCase.chunkFilePath, testCase.numOfPartitions);
//
//			// test every partition file created
//			for j, partitionFilePath := range testCase.partitionFilePaths {
//				os.Remove(partitionFilePath);
//				fileContentBytes, readErr := ioutil.ReadFile(partitionFilePath);
//				fileContent := string(fileContentBytes);
//				expectedFileContent := testCase.partitionFilesContents[j];
//				if readErr != nil {
//					t.Errorf("error reading partition '%s'\nmsg:\n%s",
//						partitionFilePath, readErr);
//					continue;
//				}
//				if fileContent != expectedFileContent {
//					t.Errorf("%s != %s", fileContent, expectedFileContent);
//				}
//			}
//		});
//	}
//}

