package shard

import (
	"bytes"
	"fmt"
	"net/http"
	"strconv"

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

func (shard *Shard) SpitOutPart(bucket string, key string, offset uint64, opts internalTypes.ObjectPartOptions) (*internalTypes.ObjectPart, error) {
	return nil, nil
}

func (shard *Shard) uploadPart(objectPart internalTypes.ObjectPart) error {
	httpClient := &http.Client{}

	shardUrl := fmt.Sprintf("http://%s:%d/shard-manager/object/part/upload", shard.Opts.IP, shard.Opts.Port)

	request, err := http.NewRequest("POST", shardUrl, bytes.NewReader(*objectPart.Data))
	if err != nil {
		fmt.Println(err)
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
		fmt.Println(err)
		return err
	}

	fmt.Println(shard, response.StatusCode)

	return nil
}
