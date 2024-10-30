package shard

import (
	"fmt"
	"net/http"

	shardManagerApi "karma8-storage/api/shard-manager"
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

func (shard *Shard) IngestPacket(packet *internalTypes.PartPacket) error {
	return shard.uploadPartPacket(packet)
}

func (shard *Shard) SpitOutPacket(bucket string, key string, offset uint64, opts internalTypes.PartPacketOptions) *internalTypes.PartPacket {
	return nil
}

func (shard *Shard) uploadPartPacket(packet *internalTypes.PartPacket) error {
	shardManagerUploadRequest := shardManagerApi.ShardManagerRequest{
		UploadOpts: shardManagerApi.UploadOptions{
			Packets: []shardManagerApi.PartPacket{
				{
					Bucket:          packet.Bucket,
					Key:             packet.Key,
					Data:            packet.Data,
					Offset:          packet.Offset,
					PacketSize:      packet.PacketSize,
					TotalObjectSize: packet.TotalObjectSize,
				},
			},
		},
		DownloadOpts: shardManagerApi.DownloadOptions{},
	}

	requestReader, err := shardManagerUploadRequest.NewReader()
	if err != nil {
		logs.ShardLogger.Println(err)
		return err
	}

	shardUrl := fmt.Sprintf("http://%s:%d/shard-manager", shard.Opts.IP, shard.Opts.Port)

	shardResponse, err := shard.Client.Post(shardUrl, "application/json", requestReader)
	if err != nil {
		logs.ShardLogger.Println(err)
		return err
	}

	shardResponseBytes := make([]byte, 10*1024)

	n, err := shardResponse.Body.Read(shardResponseBytes)
	if err != nil {
		logs.ShardLogger.Println(err)
		return err
	}

	shardUploadResponse := shardManagerApi.ShardManagerResponse{}
	shardUploadResponse.FromBytes(shardResponseBytes[0:n])

	return nil
}
