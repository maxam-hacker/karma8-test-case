package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
)

func main() {
	operation := flag.String("operation", "upload", "upload or download file")
	bucketName := flag.String("bucket", "tmp", "bucket name for uploaded object")
	keyValue := flag.String("key", "karma8/test-data/test1.data", "key value for uploaded object")
	resultFile := flag.String("result", "./test1.data", "target file to upload / result file to store object content")
	flag.Parse()

	switch *operation {
	case "upload":
		UploadFile(*bucketName, *keyValue, *resultFile)
	case "download":
		DownloadFile(*bucketName, *keyValue, *resultFile)
	default:
		fmt.Println("unknown operation")
	}
}

func DownloadFile(bucket string, objectKey string, resultFile string) {
	httpClient := http.Client{}

	request, err := http.NewRequest("POST", "http://127.0.0.1:7788/ingestor/file/download", nil)
	if err != nil {
		fmt.Println(err)
		return
	}

	request.Header.Set("X-Karma8-Object-Bucket", bucket)
	request.Header.Set("X-Karma8-Object-Key", objectKey)

	response, err := httpClient.Do(request)
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println(response.StatusCode)

	responseBytes := make([]byte, 16*1024)

	doRead := true

	totalSize := 0

	tgtFile, err := os.Create(resultFile)
	if err != nil {
		fmt.Println(err)
		return
	}

	defer tgtFile.Close()

	for doRead {
		n, err := response.Body.Read(responseBytes)
		if err != nil {
			if err == io.EOF {
				doRead = false
			} else {
				fmt.Println(err)
				break
			}
		}

		if n > 0 {
			tgtFile.Write(responseBytes[0:n])
		}

		totalSize += n
	}

	fmt.Println(totalSize)
}

func UploadFile(bucket string, objectKey string, resultFile string) {
	fileNamePath := resultFile

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
