package topology

import (
	"encoding/json"
	"os"

	"karma8-storage/ingestor/logs"
	"karma8-storage/ingestor/shards/shard"
)

type KeyShard struct {
	ObjectsShards      map[uint16]*shard.Shard
	ObjectsShardsCount uint16
}

type BucketShard struct {
	KeysShards      map[uint16]*KeyShard
	KeysShardsCount uint16
}

type Storage struct {
	BucketsShards      map[uint16]*BucketShard
	BucketsShardsCount uint16
}

var (
	DefaultConfigFilePath = "/etc/karma8/topology.config"
)

func Create(pathToConfigFile string) (*Storage, error) {
	if pathToConfigFile == "" {
		pathToConfigFile = DefaultConfigFilePath
	}

	_, err := os.Stat(pathToConfigFile)
	if err != nil {
		logs.TopologyLogger.Println(err)
		return nil, err
	}

	configContentBytes, err := os.ReadFile(pathToConfigFile)
	if err != nil {
		logs.TopologyLogger.Println(err)
		return nil, err
	}

	shardsTopology := &ShardsTopologyConfig{}

	err = json.Unmarshal(configContentBytes, shardsTopology)
	if err != nil {
		logs.TopologyLogger.Println(err)
		return nil, err
	}

	storage := &Storage{
		BucketsShards: make(map[uint16]*BucketShard),
	}

	for _, oneShardConfig := range shardsTopology.ShardsConfigs {
		bucketShard, exists := storage.BucketsShards[oneShardConfig.BucketIdx]
		if !exists {
			storage.BucketsShards[oneShardConfig.BucketIdx] = &BucketShard{
				KeysShards: make(map[uint16]*KeyShard),
			}
			storage.BucketsShardsCount++

			bucketShard = storage.BucketsShards[oneShardConfig.BucketIdx]
		}

		keyShard, exists := bucketShard.KeysShards[oneShardConfig.KeyIdx]
		if !exists {
			bucketShard.KeysShards[oneShardConfig.KeyIdx] = &KeyShard{
				ObjectsShards: make(map[uint16]*shard.Shard),
			}
			bucketShard.KeysShardsCount++

			keyShard = bucketShard.KeysShards[oneShardConfig.KeyIdx]
		}

		_, exists = keyShard.ObjectsShards[oneShardConfig.ObjectIdx]
		if !exists {
			keyShard.ObjectsShards[oneShardConfig.ObjectIdx] = shard.New(shard.ShardOptions{
				IP:        oneShardConfig.Address,
				Port:      oneShardConfig.Port,
				BucketIdx: oneShardConfig.BucketIdx,
				KeyIdx:    oneShardConfig.KeyIdx,
				ObjectIdx: oneShardConfig.ObjectIdx,
			})
			keyShard.ObjectsShardsCount++
		}
	}

	return storage, nil
}
