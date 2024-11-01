package shards

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"sort"

	"karma8-storage/ingestor/logs"
	"karma8-storage/ingestor/shards/shard"
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

func UploadPart(objectPart internalTypes.ObjectPart) error {
	if Storage == nil {
		return ErrStorageTopology
	}

	bucketSha := sha256.Sum256([]byte(objectPart.Bucket))
	bucketIdx := binary.LittleEndian.Uint16(bucketSha[:]) % Storage.BucketsShardsCount
	bucketShard, exists := Storage.BucketsShards[bucketIdx]
	if !exists {
		logs.ShardsLogger.Println(ErrBucketShardTopology)
		return ErrBucketShardTopology
	}

	keySha := sha256.Sum256([]byte(objectPart.Key))
	keyIdx := binary.LittleEndian.Uint16(keySha[:]) % bucketShard.KeysShardsCount
	keyShard, exists := Storage.BucketsShards[bucketIdx].KeysShards[keyIdx]
	if !exists {
		logs.ShardsLogger.Println(ErrKeyShardTopology)
		return ErrKeyShardTopology
	}

	packetSha := sha256.Sum256(*objectPart.Data)
	objectIdx := binary.LittleEndian.Uint16(packetSha[:]) % keyShard.ObjectsShardsCount
	objectShard, exists := Storage.BucketsShards[bucketIdx].KeysShards[keyIdx].ObjectsShards[objectIdx]
	if !exists {
		logs.ShardsLogger.Println(ErrObjectShardTopology)
		return ErrObjectShardTopology
	}

	objectPart.Opts.BucketShardsNumber = Storage.BucketsShardsCount
	objectPart.Opts.KeyShardsNumber = bucketShard.KeysShardsCount
	objectPart.Opts.ObjectShardsNumber = keyShard.ObjectsShardsCount

	return objectShard.IngestObjectPart(objectPart)
}

func DownloadPart(bucket string, key string) (chan *internalTypes.ObjectPart, error) {
	if Storage == nil {
		return nil, ErrStorageTopology
	}

	bucketSha := sha256.Sum256([]byte(bucket))
	bucketIdx := binary.LittleEndian.Uint16(bucketSha[:]) % Storage.BucketsShardsCount
	bucketShard, exists := Storage.BucketsShards[bucketIdx]
	if !exists {
		logs.ShardsLogger.Println(ErrBucketShardTopology)
		return nil, ErrBucketShardTopology
	}

	keySha := sha256.Sum256([]byte(key))
	keyIdx := binary.LittleEndian.Uint16(keySha[:]) % bucketShard.KeysShardsCount
	keyShard, exists := Storage.BucketsShards[bucketIdx].KeysShards[keyIdx]
	if !exists {
		logs.ShardsLogger.Println(ErrKeyShardTopology)
		return nil, ErrKeyShardTopology
	}

	allPartsMeta := make([]*internalTypes.ObjectPartMeta, 0)

	for _, objectShard := range keyShard.ObjectsShards {

		partsMeta, err := objectShard.SpitOutObjectMeta(bucket, key)
		if err != nil {
			return nil, ErrKeyShardTopology
		}

		allPartsMeta = append(allPartsMeta, partsMeta...)
	}

	chunks := make(chan *internalTypes.ObjectPart)

	go func() {
		sort.Slice(allPartsMeta, func(i, j int) bool {
			return allPartsMeta[i].TotalObjectOffset < allPartsMeta[j].TotalObjectOffset
		})

		for _, partMeta := range allPartsMeta {
			part, err := partMeta.Arg0.(*shard.Shard).SpitOutPart(bucket, key, partMeta.TotalObjectOffset)
			if err != nil {
				logs.ShardsLogger.Println(err)
				continue
			}

			chunks <- part
		}
	}()

	return chunks, nil
}
