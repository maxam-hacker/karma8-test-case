package main

import (
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
)

type TargetFileReader struct {
	TargetPath string
	file       *os.File
}

func NewTargetFileReader(targetPath string) (*TargetFileReader, error) {
	file, err := os.Open(targetPath)
	if err != nil {
		return nil, err
	}

	return &TargetFileReader{
		TargetPath: targetPath,
		file:       file,
	}, nil
}

func (reader TargetFileReader) Read(p []byte) (n int, err error) {
	return reader.file.Read(p)
}

func main() {
	operation := flag.String("operation", "upload", "upload or download file")
	bucketName := flag.String("bucket", "tmp", "bucket name for uploaded object")
	keyValue := flag.String("key", "karma8/test-data/test1.data", "key value for uploaded object")
	targetFile := flag.String("target", "./test1.data", "target file to upload / result file to store object content")
	flag.Parse()

	switch *operation {
	case "upload":
		UploadFile(*bucketName, *keyValue, *targetFile)
	case "download":
		DownloadFile(*bucketName, *keyValue, *targetFile)
	default:
		fmt.Println("unknown operation")
	}
}

func DownloadFile(bucket string, objectKey string, targetFile string) {
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

	tgtFile, err := os.Create(targetFile)
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

		//time.Sleep(1 * time.Second)

		totalSize += n
	}

	fmt.Println(totalSize)
}

func UploadFile(bucket string, objectKey string, targetFile string) {
	httpClient := &http.Client{}

	testFileInfo, err := os.Stat(targetFile)
	if err != nil {
		fmt.Println(err)
		return
	}

	reader, err := NewTargetFileReader(targetFile)
	if err != nil {
		fmt.Println(err)
		return
	}

	request, err := http.NewRequest("POST", "http://127.0.0.1:7788/ingestor/file/upload", reader)
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
