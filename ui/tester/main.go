package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"path"

	ingestorApi "karma8-storage/api/ingestor"
)

var (
	testDataDir = "/tmp/karma8/test-data"

	dataBuffer = make([]byte, 10*1024)
)

func main() {
	CreateFakeFiles()

	SendByChunks()
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

		response, err := http.DefaultClient.Post("http://127.0.0.1:7788/ingestor", "application/json", requestReader)
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

	fakeData := make([]byte, 1*1024*1024)
	for idx := 0; idx < len(fakeData); idx++ {
		fakeData[idx] = byte(idx % 255)
	}

	_, err = fakeFile.Write(fakeData)
	if err != nil {
		return err
	}

	return nil
}
