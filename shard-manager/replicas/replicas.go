package replicas

import (
	"errors"
	"os"
	"path"
	"strings"

	internalTypes "karma8-storage/internals/types"
	"karma8-storage/shard-manager/logs"
	"karma8-storage/shard-manager/replicas/replica"
)

var (
	ErrWritingError = errors.New("error while writing packet to replica")
	ErrReadingError = errors.New("error while reading packet from replica")

	replicas []*replica.ShardReplica
)

func Initialize() {
	logs.ReplicasLogger.Println("initialize shard replicas...")

	replicasBasePath := os.Getenv("REPLICAS_BASE_PATH")
	replicasIndex := os.Getenv("REPLICAS_INDEX")
	replicasPaths := os.Getenv("REPLICAS_PATHS")

	if replicasIndex == "{{.Task.Slot}}" {
		logs.ReplicasLogger.Println("changing replicas index...")
		replicasIndex, _ = os.Hostname()
	}

	logs.ReplicasLogger.Println(replicasBasePath, replicasIndex, replicasPaths)

	launchReplicas(replicasBasePath, replicasIndex, replicasPaths)
}

func launchReplicas(basePath string, index string, paths string) {
	for _, replicaPath := range strings.Split(paths, ";") {
		replica, err := New(path.Join(basePath, index, replicaPath))
		if err != nil {
			logs.ReplicasLogger.Println(err)
			continue
		}
		replicas = append(replicas, replica)
	}
}

func deleteReplicas() {
	for _, replica := range replicas {
		replica.DeleteReplica()
	}
}

func New(replicaPath string) (*replica.ShardReplica, error) {
	replica := &replica.ShardReplica{
		BasePath: replicaPath,
	}

	err := os.MkdirAll(replica.BasePath, os.ModePerm)
	if err != nil {
		logs.ReplicasLogger.Println(err)
		return nil, err
	}

	return replica, nil
}

func WritePacket(packet *internalTypes.PartPacket) error {
	for _, replica := range replicas {
		replica.WritePacket(packet)
	}

	return nil
}

func ReadPacket(objectBucket string, objectKey string, objectOffset uint64) (*internalTypes.PartPacket, error) {
	for _, replica := range replicas {
		packet, err := replica.ReadPacket(objectBucket, objectKey, objectOffset)
		if err != nil {
			logs.ReplicasLogger.Println(err)
			continue
		}
		return packet, nil
	}

	return nil, ErrReadingError
}
