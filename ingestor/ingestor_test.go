package main

import (
	"io"
	"net/http"
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"

	ingestorApi "karma8-storage/api/ingestor"
	"karma8-storage/ingestor/shards"
)

var (
	testDataDir = "/tmp/karma8/test-data"
)

func TestMain(m *testing.M) {
	os.Setenv("INGESTOR_SERVICE_ADDR", "0.0.0.0")
	os.Setenv("INGESTOR_SERVICE_PORT", "7780")

	os.MkdirAll(testDataDir, os.ModePerm)

	err := CreateFakeFiles()
	if err != nil {
		os.Exit(-1)
	}

	m.Run()

	os.RemoveAll(testDataDir)

	os.Exit(0)
}

func CreateFakeFiles() error {
	fakeFile, err := os.Create(path.Join(testDataDir, "test1.data"))
	if err != nil {
		return err
	}

	fakeData := make([]byte, 25*1024*1024)
	for idx := 0; idx < len(fakeData); idx++ {
		fakeData[idx] = byte(idx % 255)
	}

	_, err = fakeFile.Write(fakeData)
	if err != nil {
		return err
	}

	return nil
}

func Test0001(t *testing.T) {
	shards.Initialize("./topology.config")

	go runRestServer()

	testFilePath := path.Join(testDataDir, "test1.data")

	testFileInfo, err := os.Stat(testFilePath)
	assert.NoError(t, err)

	testFile, err := os.Open(testFilePath)
	assert.NoError(t, err)

	dataBuffer := make([]byte, 4096)

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
		assert.NoError(t, err)

		response, err := http.DefaultClient.Post("http://127.0.0.1:7780/ingestor", "application/json", requestReader)
		assert.NoError(t, err)
		assert.Equal(t, 200, response.StatusCode)

		offset += uint64(n)
	}
}
