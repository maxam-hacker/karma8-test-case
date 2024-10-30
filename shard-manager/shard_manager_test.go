package main

import (
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	shardManagerApi "karma8-storage/api/shard-manager"
	"karma8-storage/shard-manager/replicas"
)

func TestMain(m *testing.M) {
	os.Setenv("SHARD_MANAGER_SERVICE_ADDR", "0.0.0.0")
	os.Setenv("SHARD_MANAGER_SERVICE_PORT", "7788")
	os.Setenv("REPLICAS_BASE_PATHS", "/tmp/karma8/test3-4-5/r1;/tmp/karma8/test3-4-5/r2")

	m.Run()

	os.RemoveAll("/tmp/karma8/test3-4-5")

	os.Exit(0)
}

func Test0001(t *testing.T) {
	replicas.Initialize()

	packetData := []byte("Test case 3 fake data")

	uploadRequest := shardManagerApi.ShardManagerRequest{
		UploadOpts: shardManagerApi.UploadOptions{
			Packets: []shardManagerApi.PartPacket{
				{
					Bucket:          "test-case-3-bucket",
					Key:             "test-case-3-key",
					Data:            packetData,
					Offset:          0,
					PacketSize:      uint64(len(packetData)),
					TotalObjectSize: uint64(len(packetData)),
				},
			},
		},
	}

	uploadResponse, err := uploadPackage(&uploadRequest.UploadOpts)
	assert.NoError(t, err)
	assert.NotEmpty(t, uploadResponse.Packets)

	downloadRequest := shardManagerApi.ShardManagerRequest{
		DownloadOpts: shardManagerApi.DownloadOptions{
			Packets: []shardManagerApi.PartPacket{
				{
					Bucket: "test-case-3-bucket",
					Key:    "test-case-3-key",
					Offset: 0,
				},
			},
		},
	}

	downloadResponse, err := downloadPackage(&downloadRequest.DownloadOpts)
	assert.NoError(t, err)
	assert.NotEmpty(t, downloadResponse.Packets)
	assert.Equal(t, packetData, downloadResponse.Packets[0].Data)
}

func Test0002(t *testing.T) {
	replicas.Initialize()

	go runRestServer()

	time.Sleep(3 * time.Second)

	packetData := []byte("Test case 4 fake data")

	shardManagerUploadRequest := shardManagerApi.ShardManagerRequest{
		UploadOpts: shardManagerApi.UploadOptions{
			Packets: []shardManagerApi.PartPacket{
				{
					Bucket:          "test-case-4-bucket",
					Key:             "test-case-4-key",
					Data:            packetData,
					Offset:          0,
					PacketSize:      uint64(len(packetData)),
					TotalObjectSize: uint64(len(packetData)),
				},
			},
		},
		DownloadOpts: shardManagerApi.DownloadOptions{},
	}

	requestReader, err := shardManagerUploadRequest.NewReader()
	assert.NoError(t, err)

	uploadResponse, err := http.DefaultClient.Post("http://127.0.0.1:7788/shard-manager", "application/json", requestReader)
	assert.NoError(t, err)
	assert.Equal(t, 200, uploadResponse.StatusCode)

	shardManagerDownloadRequest := shardManagerApi.ShardManagerRequest{
		UploadOpts: shardManagerApi.UploadOptions{},
		DownloadOpts: shardManagerApi.DownloadOptions{
			Packets: []shardManagerApi.PartPacket{
				{
					Bucket: "test-case-4-bucket",
					Key:    "test-case-4-key",
					Offset: 0,
				},
			},
		},
	}

	requestReader, err = shardManagerDownloadRequest.NewReader()
	assert.NoError(t, err)

	downloadResponse, err := http.DefaultClient.Post("http://127.0.0.1:7788/shard-manager", "application/json", requestReader)
	assert.NoError(t, err)
	assert.Equal(t, 200, downloadResponse.StatusCode)

	dowloadResponseBytes := make([]byte, 4096)

	n, _ := downloadResponse.Body.Read(dowloadResponseBytes)
	assert.NotEqual(t, n, 0)

	dowloadShardManagerResponse := shardManagerApi.ShardManagerResponse{}
	dowloadShardManagerResponse.FromBytes(dowloadResponseBytes[0:n])
	assert.Equal(t, len(dowloadShardManagerResponse.Packets), 1)
	assert.Equal(t, dowloadShardManagerResponse.Packets[0].Data, packetData)
}
