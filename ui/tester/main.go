package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"path"

	ingestorApi "karma8-storage/api/ingestor"
)

var (
	testDataDir = "/tmp/karma8/test-data"

	dataBuffer = make([]byte, 10*1024*1024)
)

func main() {
	transferType := flag.String("transfer", "stream", "how to transfer target file")
	operationType := flag.String("operation", "upload", "upload or download file")
	//targetFileName := flag.String("file", "test1.data", "upload or download file")
	fakeFile := flag.String("fakeFile", "none", "create fake data file")

	if *fakeFile != "none" {
		CreateFakeFiles()
	}

	if *operationType == "upload" {
		switch *transferType {
		case "stream":
			SendByStream()
		case "chunks":
			SendByChunks()
		case "multipart":
			SendByMultiPart()
		}
	}
}

func SendByStream() {
	httpClient := &http.Client{}

	testFilePath := path.Join(testDataDir, "test1.data")

	_, err := os.Stat(testFilePath)
	if err != nil {
		fmt.Println(err)
		return
	}

	testFileBytes, err := os.ReadFile(testFilePath)
	if err != nil {
		fmt.Println(err)
		return
	}

	request, err := http.NewRequest("POST", "http://127.0.0.1:7788/ingestor/stream", bytes.NewReader(testFileBytes))
	if err != nil {
		fmt.Println(err)
		return
	}
	request.Header.Set("Content-Type", "application/octet-stream")

	response, err := httpClient.Do(request)
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println(response.StatusCode)
}

func SendByMultiPart() {
	var b bytes.Buffer

	multiPartWiter := multipart.NewWriter(&b)
	multiPartWiter.SetBoundary("bla-bla-bla")
	defer multiPartWiter.Close()

	testFilePath := path.Join(testDataDir, "test1.data")

	_, err := os.Stat(testFilePath)
	if err != nil {
		fmt.Println(err)
		return
	}

	testFile, err := os.Open(testFilePath)
	if err != nil {
		fmt.Println(err)
		return
	}

	mulipartIOWriter, err := multiPartWiter.CreateFormFile("file", testFile.Name())
	if err != nil {
		fmt.Println(err)
		return
	}

	n, err := io.Copy(mulipartIOWriter, testFile)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(n)

	request, err := http.NewRequest("POST", "http://127.0.0.1:7788/ingestor/multipart", &b)
	if err != nil {
		fmt.Println(err)
		return
	}

	request.Header.Set("Content-Type", multiPartWiter.FormDataContentType())

	httpClient := &http.Client{}

	response, err := httpClient.Do(request)
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println(response.StatusCode)
}

func SendByChunks() {
	testFilePath := path.Join(testDataDir, "test1.data")

	testFileInfo, err := os.Stat(testFilePath)
	if err != nil {
		fmt.Println(err)
		return
	}

	testFile, err := os.Open(testFilePath)
	if err != nil {
		fmt.Println(err)
		return
	}

	doRead := true

	offset := uint64(0)

	httpClient := &http.Client{}

	for doRead {
		n, err := testFile.Read(dataBuffer)
		if n == 0 {
			break
		}

		if err == io.EOF {
			doRead = false
		}

		request := ingestorApi.IngestorRequest{
			UploadOpts: ingestorApi.UploadOptions{},
			UploadChankedOpts: ingestorApi.UploadChankedOptions{
				Chunk: ingestorApi.Chunk{
					Bucket:    "test-6-bucket",
					Key:       "test-6-key",
					Offset:    offset,
					Data:      dataBuffer[0:n],
					ChunkSize: uint64(n),
					TotalSize: uint64(testFileInfo.Size()),
				},
			},
		}

		requestReader, err := request.NewReader()
		if err != nil {
			fmt.Println(err)
			return
		}

		fmt.Println(len(requestReader.RequestBytes))

		req, err := http.NewRequest("POST", "http://127.0.0.1:7788/ingestor", requestReader)
		if err != nil {
			fmt.Println(err)
			return
		}
		req.Header.Set("Content-Type", "application/json")

		response, err := httpClient.Do(req)
		if err != nil {
			fmt.Println(err)
			return
		}

		fmt.Println(response)

		offset += uint64(n)
	}
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
