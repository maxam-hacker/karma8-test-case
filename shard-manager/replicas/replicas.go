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
	ErrObjectPartWritingError     = errors.New("error while writing object part to replica")
	ErrObjectPartReadingError     = errors.New("error while reading object part from replica")
	ErrObjectPartMetaWritingError = errors.New("error while writing object part meta to replica")
	ErrObjectPartMetaReadingError = errors.New("error while reading object part meta from replica")

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

func WriteObjectPart(objectPart internalTypes.ObjectPart) error {
	for _, replica := range replicas {
		replica.WriteObjectPart(objectPart)
	}

	return nil
}

func ReadObjectPartsMeta(objectBucket string, objectKey string) ([]*internalTypes.ObjectPartMeta, error) {
	for _, replica := range replicas {
		objectPartsMeta, err := replica.ReadObjectPartsMeta(objectBucket, objectKey)
		if err != nil {
			logs.ReplicasLogger.Println(err)
			continue
		}
		return objectPartsMeta, nil
	}

	return nil, ErrObjectPartMetaReadingError
}

func ReadObjectPart(objectBucket string, objectKey string, totalObjectOffset uint64) (*internalTypes.ObjectPart, error) {
	for _, replica := range replicas {
		objectPart, err := replica.ReadObjectPart(objectBucket, objectKey, totalObjectOffset)
		if err != nil {
			logs.ReplicasLogger.Println(err)
			continue
		}
		return objectPart, nil
	}

	return nil, ErrObjectPartReadingError
}
