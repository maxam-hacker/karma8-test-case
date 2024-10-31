package main

import (
	"bytes"
	"flag"
	"fmt"
	"net/http"
	"os"
	"path"
	"strconv"
)

var (
	testDataDir = "/tmp/karma8/test-data"
)

func main() {
	operation := flag.String("operation", "upload", "upload or download file")
	bucketName := flag.String("bucket", "/tmp", "upload or download file")
	objectKey := flag.String("objectKey", "/karma8/test-data/test1.data", "create fake data file")

	CreateFakeFiles()

	switch *operation {
	case "upload":
		UploadFile(*bucketName, *objectKey)
	case "download":
		DownloadFile(path.Join(*bucketName, *objectKey))
	}
}

func DownloadFile(fileName string) {
}

func UploadFile(bucket string, objectKey string) {
	fileNamePath := path.Join(bucket, objectKey)

	httpClient := &http.Client{}

	testFileInfo, err := os.Stat(fileNamePath)
	if err != nil {
		fmt.Println(err)
		return
	}

	testFileBytes, err := os.ReadFile(fileNamePath)
	if err != nil {
		fmt.Println(err)
		return
	}

	request, err := http.NewRequest("POST", "http://127.0.0.1:7788/ingestor/file/upload", bytes.NewReader(testFileBytes))
	if err != nil {
		fmt.Println(err)
		return
	}
	request.Header.Set("Content-Type", "application/octet-stream")
	request.Header.Set("X-Karma8-Object-Bucket", bucket)
	request.Header.Set("X-Karma8-Object-Key", objectKey)
	request.Header.Set("X-Karma8-Object-Total-Size", strconv.Itoa(int(testFileInfo.Size())))

	response, err := httpClient.Do(request)
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println(response.StatusCode)
}

func CreateFakeFiles() error {
	err := os.MkdirAll(testDataDir, os.ModePerm)
	if err != nil {
		fmt.Println(err)
		return err
	}

	fakeFile, err := os.Create(path.Join(testDataDir, "test1.data"))
	if err != nil {
		return err
	}

	fakeData := make([]byte, 30*1024*1024)
	for idx := 0; idx < len(fakeData); idx++ {
		fakeData[idx] = byte(idx % 255)
	}

	_, err = fakeFile.Write(fakeData)
	if err != nil {
		return err
	}

	return nil
}
