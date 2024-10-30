package shards

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"karma8-storage/ingestor/logs"
	shardsTopology "karma8-storage/ingestor/shards/topology"
	internalTypes "karma8-storage/internals/types"
)

var (
	ErrStorageTopology     = errors.New("failed to create storage topology")
	ErrBucketShardTopology = errors.New("failed to get bucket topology")
	ErrKeyShardTopology    = errors.New("failed to get key topology")
	ErrObjectShardTopology = errors.New("failed to get object topology")

	Storage *shardsTopology.Storage
)

func Initialize(shardsConfigFilePath string) {
	logs.ShardsLogger.Println("initialize topology...")

	storageTopology, err := shardsTopology.Create(shardsConfigFilePath)
	if err != nil {
		logs.ShardsLogger.Println(err)
		return
	}

	Storage = storageTopology
}

func UploadPartPacket(packet *internalTypes.PartPacket) error {
	if Storage == nil {
		return ErrStorageTopology
	}

	bucketSha := sha256.Sum256([]byte(packet.Bucket))
	bucketIdx := binary.LittleEndian.Uint16(bucketSha[:]) % Storage.BucketsShardsCount
	bucketShard, exists := Storage.BucketsShards[bucketIdx]
	if !exists {
		logs.ShardsLogger.Println(ErrBucketShardTopology)
		return ErrBucketShardTopology
	}

	keySha := sha256.Sum256([]byte(packet.Key))
	keyIdx := binary.LittleEndian.Uint16(keySha[:]) % bucketShard.KeysShardsCount
	keyShard, exists := Storage.BucketsShards[bucketIdx].KeysShards[keyIdx]
	if !exists {
		logs.ShardsLogger.Println(ErrKeyShardTopology)
		return ErrKeyShardTopology
	}

	packetSha := sha256.Sum256(packet.Data)
	objectIdx := binary.LittleEndian.Uint16(packetSha[:]) % keyShard.ObjectsShardsCount
	objectShard, exists := Storage.BucketsShards[bucketIdx].KeysShards[keyIdx].ObjectsShards[objectIdx]
	if !exists {
		logs.ShardsLogger.Println(ErrObjectShardTopology)
		return ErrObjectShardTopology
	}

	packet.Opts.BucketShardsNumber = Storage.BucketsShardsCount
	packet.Opts.KeyShardsNumber = bucketShard.KeysShardsCount
	packet.Opts.ObjectShardsNumber = keyShard.ObjectsShardsCount

	return objectShard.IngestPacket(packet)
}
