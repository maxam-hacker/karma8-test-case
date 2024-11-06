package replicas

import (
	"errors"
	"os"
	"path"
	"strings"

	internalTypes "karma8-storage/internals/types"
	"karma8-storage/shard-manager/logs"
	oneReplica "karma8-storage/shard-manager/replicas/replica"
)

var (
	ErrObjectPartWriting     = errors.New("error while writing object part to replica")
	ErrObjectPartReading     = errors.New("error while reading object part from replica")
	ErrObjectPartMetaReading = errors.New("error while reading object part meta from replica")

	ErrObjectIsNotPresent = errors.New("object is not present")

	replicas []*oneReplica.ShardReplica
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

func New(replicaPath string) (*oneReplica.ShardReplica, error) {
	replica := &oneReplica.ShardReplica{
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
	errorsCounter := 0

	for _, replica := range replicas {
		err := replica.WriteObjectPart(objectPart)
		if err != nil {
			replica.LastError = err
			errorsCounter++
		}
	}

	if errorsCounter == len(replicas) {
		return ErrObjectPartWriting
	}

	return nil
}

func ReadObjectPartsMeta(objectBucket string, objectKey string) ([]internalTypes.ObjectPartMeta, error) {
	for _, replica := range replicas {
		objectPartsMeta, err := replica.ReadObjectPartsMeta(objectBucket, objectKey)
		if err != nil {
			replica.LastError = err
			continue
		}
		return objectPartsMeta, nil
	}

	for _, replica := range replicas {
		if replica.LastError == oneReplica.ErrObjectMetaFolder {
			return nil, ErrObjectIsNotPresent
		}
	}

	return nil, ErrObjectPartMetaReading
}

func ReadObjectPart(objectBucket string, objectKey string, totalObjectOffset uint64) (*internalTypes.ObjectPart, error) {
	for _, replica := range replicas {
		objectPart, err := replica.ReadObjectPart(objectBucket, objectKey, totalObjectOffset)
		if err != nil {
			replica.LastError = err
			continue
		}
		return objectPart, nil
	}

	for _, replica := range replicas {
		if replica.LastError == oneReplica.ErrObjectKeyFolder {
			return nil, ErrObjectIsNotPresent
		}
	}

	return nil, ErrObjectPartReading
}

func deleteReplicas() {
	for _, replica := range replicas {
		replica.DeleteReplica()
	}
}
