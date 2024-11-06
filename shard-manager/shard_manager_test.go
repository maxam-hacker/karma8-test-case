package main

import (
	"bytes"
	"net/http"
	"os"
	"strconv"
	"testing"
	"time"

	internalTypes "karma8-storage/internals/types"
	"karma8-storage/shard-manager/replicas"

	"github.com/stretchr/testify/assert"
)

func TestMain(m *testing.M) {
	os.Setenv("SHARD_MANAGER_SERVICE_ADDR", "0.0.0.0")
	os.Setenv("SHARD_MANAGER_SERVICE_PORT", "7788")

	os.Setenv("REPLICAS_BASE_PATH", "/tmp/karma8-integration-tests")
	os.Setenv("REPLICAS_INDEX", "1")
	os.Setenv("REPLICAS_PATHS", "r1;r2;r3")
	replicas.Initialize()

	go runRestServer()

	time.Sleep(2 * time.Second)

	m.Run()

	os.RemoveAll("/tmp/karma8-integration-tests")

	os.Exit(0)
}

func Test0001(t *testing.T) {
	partData := []byte("Replica test1 data smple")
	totalObjectSize := len(partData)

	partBucket := "karma8-test-case-0-bucket"
	partKey := "replica-test-case-0-key"

	objectPart := internalTypes.ObjectPart{
		Bucket:            partBucket,
		Key:               partKey,
		Data:              &partData,
		PartDataSize:      uint64(len(partData)),
		TotalObjectOffset: 0,
		TotalObjectSize:   uint64(totalObjectSize),
		Opts:              internalTypes.ObjectPartOptions{},
	}

	httpClient := &http.Client{}

	request, err := http.NewRequest("POST", "http://127.0.0.1:7788/shard-manager/object/part/upload", bytes.NewReader(*objectPart.Data))
	assert.NoError(t, err)

	request.Header.Set("Content-Type", "application/octet-stream")
	request.Header.Set("X-Karma8-Object-Bucket", objectPart.Bucket)
	request.Header.Set("X-Karma8-Object-Key", objectPart.Key)
	request.Header.Set("X-Karma8-Object-Part-Data-Size", strconv.Itoa(int(objectPart.PartDataSize)))
	request.Header.Set("X-Karma8-Object-Total-Offset", strconv.Itoa(int(objectPart.TotalObjectOffset)))
	request.Header.Set("X-Karma8-Object-Total-Size", strconv.Itoa(int(objectPart.TotalObjectSize)))

	uploadResponse, err := httpClient.Do(request)
	assert.NoError(t, err)
	assert.Equal(t, uploadResponse.StatusCode, 200)
}
