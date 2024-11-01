package replica

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path"
	"strconv"
	"strings"

	internalTypes "karma8-storage/internals/types"
	"karma8-storage/shard-manager/logs"
)

type ShardReplica struct {
	BasePath string
}

var (
	ErrEmptyOffset = errors.New("can't find file for offset")
)

func (replica *ShardReplica) WriteObjectPart(objectPart internalTypes.ObjectPart) error {
	pathToKey := replica.getPathToKey(objectPart.Bucket, objectPart.Key)

	err := os.MkdirAll(pathToKey, os.ModePerm)
	if err != nil {
		logs.ReplicaLogger.Println(err)
		return err
	}

	objectPartBytes, err := objectPart.GetBytes()
	if err != nil {
		logs.ReplicaLogger.Println(err)
		return err
	}

	err = os.WriteFile(fmt.Sprintf("%s/%d", pathToKey, objectPart.TotalObjectOffset), objectPartBytes, os.ModePerm)
	if err != nil {
		logs.ReplicaLogger.Println(err)
		return err
	}

	objectPartMetaBytes, err := objectPart.GetMetaBytes()
	if err != nil {
		logs.ReplicaLogger.Println(err)
		return err
	}

	err = os.WriteFile(fmt.Sprintf("%s/%d.meta", pathToKey, objectPart.TotalObjectOffset), objectPartMetaBytes, os.ModePerm)
	if err != nil {
		logs.ReplicaLogger.Println(err)
		return err
	}

	return nil
}

func (replica *ShardReplica) ReadObjectPartsMeta(objectBucket string, objectKey string) ([]*internalTypes.ObjectPartMeta, error) {
	pathToKey := replica.getPathToKey(objectBucket, objectKey)

	files, err := os.ReadDir(pathToKey)
	if err != nil {
		logs.ReplicaLogger.Println(err)
		return nil, err
	}

	objectPartsMeta := make([]*internalTypes.ObjectPartMeta, 0)

	for _, file := range files {
		if !strings.Contains(file.Name(), ".meta") {
			continue
		}

		objectPartMetaBytes, err := os.ReadFile(path.Join(pathToKey, file.Name()))
		if err != nil {
			logs.ReplicaLogger.Println(err)
			continue
		}

		objectPartMeta := &internalTypes.ObjectPartMeta{}

		err = json.Unmarshal(objectPartMetaBytes, objectPartMeta)
		if err != nil {
			logs.ReplicaLogger.Println(err)
			continue
		}

		objectPartsMeta = append(objectPartsMeta, objectPartMeta)
	}

	return objectPartsMeta, nil
}

func (replica *ShardReplica) ReadObjectPart(objectBucket string, objectKey string, totalObjectOffset uint64) (*internalTypes.ObjectPart, error) {
	pathToKey := replica.getPathToKey(objectBucket, objectKey)

	files, err := os.ReadDir(pathToKey)
	if err != nil {
		logs.ReplicaLogger.Println(err)
		return nil, err
	}

	targetFound := false

	for _, file := range files {
		fileOffset, err := strconv.ParseUint(file.Name(), 10, 1)
		if err != nil {
			logs.ReplicaLogger.Println(err)
			return nil, err
		}

		if fileOffset == totalObjectOffset {
			targetFound = true
			break
		}
	}

	if !targetFound {
		return nil, ErrEmptyOffset
	}

	objectPartBytes, err := os.ReadFile(fmt.Sprintf("%s/%d", pathToKey, totalObjectOffset))
	if err != nil {
		logs.ReplicaLogger.Println(err)
		return nil, err
	}

	objectPart := &internalTypes.ObjectPart{}

	err = json.Unmarshal(objectPartBytes, objectPart)
	if err != nil {
		logs.ReplicaLogger.Println(err)
		return nil, err
	}

	return objectPart, nil
}

func (replica *ShardReplica) DeleteKey(objectBucket string, objectKey string) error {
	pathToKey := replica.getPathToKey(objectBucket, objectKey)

	err := os.RemoveAll(pathToKey)
	if err != nil {
		logs.ReplicaLogger.Println(err)
		return err
	}

	return nil
}

func (replica *ShardReplica) DeleteBucket(objectBucket string) error {
	pathToBucket := replica.getPathToBucket(objectBucket)

	err := os.RemoveAll(pathToBucket)
	if err != nil {
		logs.ReplicaLogger.Println(err)
		return err
	}

	return nil
}

func (replica *ShardReplica) DeleteReplica() error {
	err := os.RemoveAll(replica.BasePath)
	if err != nil {
		logs.ReplicaLogger.Println(err)
		return err
	}

	return nil
}

func (replica *ShardReplica) getPathToKey(objectBucket string, objectKey string) string {
	return path.Join(replica.BasePath, objectBucket, objectKey)
}

func (replica *ShardReplica) getPathToBucket(objectBucket string) string {
	return path.Join(replica.BasePath, objectBucket)
}
