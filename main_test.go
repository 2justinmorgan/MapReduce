package main

import (
	"fmt"
	"testing"
	"io/ioutil"
)

func TestGetChunkFileName(t *testing.T) {
	var testCases = []struct {
		fpath string
		fnum, M int
		expect string
	}{
		{
			"./this/one.txt",
			7,
			8,
			"input_files/chunks/one.txt_chunk_007_of_008.txt",
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
		testName := fmt.Sprintf("test%d %s",i,testCase.fpath[0:10]);
		t.Run(testName, func(t *testing.T) {
			actual := getChunkFileName(testCase.fpath,testCase.fnum,testCase.M);
			if actual != testCase.expect {
				t.Errorf("%s != %s", actual, testCase.expect);
			}
		});
	}
}

func fwriteForTesting(fpath string, content string) error {
   err := ioutil.WriteFile(fpath, []byte(content), 0644)
	if err != nil {
		return err;
	}
	return nil;
}

func TestFwriteForTesting(t *testing.T) {
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
			writeErr := fwriteForTesting(testCase.fpath, testCase.inputContent);
			fileContentBytes, readErr := ioutil.ReadFile(testCase.fpath);
			fileContent := string(fileContentBytes);
			if writeErr != nil {
				t.Errorf("error writing '%s'\nmsg:\n%s", testCase.fpath, writeErr);
			} else if readErr != nil {
				t.Errorf("error reading '%s'\nmsg:\n%s", testCase.fpath, readErr);
			} else if fileContent != testCase.inputContent {
				t.Errorf("%s != %s", fileContent, testCase.inputContent);
			}
		});
	}
}

