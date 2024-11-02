package shard

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"

	"karma8-storage/ingestor/logs"
	internalTypes "karma8-storage/internals/types"
)

type ShardOptions struct {
	IP        string
	Port      uint16
	BucketIdx uint16
	KeyIdx    uint16
	ObjectIdx uint16
}

type Shard struct {
	Opts   ShardOptions
	Client *http.Client
}

func New(opts ShardOptions) *Shard {
	shard := &Shard{
		Opts:   opts,
		Client: &http.Client{},
	}

	return shard
}

func (shard *Shard) IngestObjectPart(objectPart internalTypes.ObjectPart) error {
	return shard.uploadPart(objectPart)
}

func (shard *Shard) SpitOutPart(bucket string, key string, offset uint64) (*internalTypes.ObjectPart, error) {
	logs.ShardLogger.Println("uploading part...")

	httpClient := &http.Client{}

	shardUrl := fmt.Sprintf("http://%s:%d/shard-manager/object/part/download", shard.Opts.IP, shard.Opts.Port)

	request, err := http.NewRequest("POST", shardUrl, nil)
	if err != nil {
		logs.ShardLogger.Println(err)
		return nil, err
	}

	request.Header.Set("X-Karma8-Object-Bucket", bucket)
	request.Header.Set("X-Karma8-Object-Key", key)
	request.Header.Set("X-Karma8-Object-Total-Offset", strconv.Itoa(int(offset)))

	response, err := httpClient.Do(request)
	if err != nil {
		logs.ShardLogger.Println(err)
		return nil, err
	}

	objectPart := &internalTypes.ObjectPart{
		Bucket: bucket,
		Data:   &[]byte{},
	}

	responseBytes := make([]byte, 16*1024)

	doRead := true

	for doRead {
		n, err := response.Body.Read(responseBytes)
		if err != nil {
			if err != io.EOF {
				logs.ShardLogger.Println(err)
				break
			} else {
				doRead = false
			}
		}

		*objectPart.Data = append(*objectPart.Data, responseBytes[0:n]...)
	}

	logs.ShardLogger.Println(response.StatusCode)

	return objectPart, nil
}

func (shard *Shard) SpitOutObjectMeta(bucket string, key string) ([]internalTypes.ObjectPartMeta, error) {
	logs.ShardLogger.Println("object meta...")

	partsMeta := make([]internalTypes.ObjectPartMeta, 0)

	httpClient := &http.Client{}

	shardUrl := fmt.Sprintf("http://%s:%d/shard-manager/object/meta", shard.Opts.IP, shard.Opts.Port)

	request, err := http.NewRequest("POST", shardUrl, nil)
	if err != nil {
		logs.ShardLogger.Println(err)
		return partsMeta, err
	}
	request.Header.Set("X-Karma8-Object-Bucket", bucket)
	request.Header.Set("X-Karma8-Object-Key", key)

	response, err := httpClient.Do(request)
	if err != nil {
		logs.ShardLogger.Println(err)
		return partsMeta, err
	}

	if response.Header.Get("X-Karma8-Ingestor-Service-Error") != "" {
		logs.ShardLogger.Println(response.Header.Get("X-Karma8-Ingestor-Service-Error"))
		return partsMeta, err
	}

	responseBytes, err := io.ReadAll(response.Body)
	if err != nil {
		logs.ShardLogger.Println(err)
		return partsMeta, nil
	}

	err = json.Unmarshal(responseBytes, &partsMeta)
	if err != nil {
		logs.ShardLogger.Println(err)
		return partsMeta, nil
	}

	return partsMeta, nil
}

func (shard *Shard) uploadPart(objectPart internalTypes.ObjectPart) error {
	logs.ShardLogger.Println("uploading part...")

	httpClient := &http.Client{}

	shardUrl := fmt.Sprintf("http://%s:%d/shard-manager/object/part/upload", shard.Opts.IP, shard.Opts.Port)

	request, err := http.NewRequest("POST", shardUrl, bytes.NewReader(*objectPart.Data))
	if err != nil {
		logs.ShardLogger.Println(err)
		return err
	}
	request.Header.Set("Content-Type", "application/octet-stream")
	request.Header.Set("X-Karma8-Object-Bucket", objectPart.Bucket)
	request.Header.Set("X-Karma8-Object-Key", objectPart.Key)
	request.Header.Set("X-Karma8-Object-Part-Data-Size", strconv.Itoa(int(objectPart.PartDataSize)))
	request.Header.Set("X-Karma8-Object-Total-Offset", strconv.Itoa(int(objectPart.TotalObjectOffset)))
	request.Header.Set("X-Karma8-Object-Total-Size", strconv.Itoa(int(objectPart.TotalObjectSize)))

	response, err := httpClient.Do(request)
	if err != nil {
		logs.ShardLogger.Println(err)
		return err
	}

	logs.ShardLogger.Println(response.StatusCode)

	return nil
}
