package replica

import (
	"errors"
	"fmt"
	"os"
	"path"
	"strconv"

	internalTypes "karma8-storage/internals/types"
	"karma8-storage/shard-manager/logs"
)

type ShardReplica struct {
	BasePath string
}

var (
	ErrEmptyOffset = errors.New("can't find file for offset")
)

func (replica *ShardReplica) WritePacket(packet *internalTypes.PartPacket) error {
	pathToKey := replica.getPathToKey(packet.Bucket, packet.Key)

	err := os.MkdirAll(pathToKey, os.ModePerm)
	if err != nil {
		logs.ReplicaLogger.Println(err)
		return err
	}

	data, err := packet.GetBytes()
	if err != nil {
		logs.ReplicaLogger.Println(err)
		return err
	}

	err = os.WriteFile(fmt.Sprintf("%s/%d", pathToKey, packet.Offset), data, os.ModePerm)
	if err != nil {
		logs.ReplicaLogger.Println(err)
		return err
	}

	return nil
}

func (replica *ShardReplica) ReadPacket(objectBucket string, objectKey string, objectOffset uint64) (*internalTypes.PartPacket, error) {
	pathToKey := path.Join(replica.BasePath, objectBucket, objectKey)

	files, err := os.ReadDir(pathToKey)
	if err != nil {
		logs.ReplicaLogger.Println(err)
		return nil, err
	}

	minDistance := uint64(0)
	targetOffset := objectOffset
	targetFound := false

	for _, file := range files {
		fileOffset, err := strconv.ParseUint(file.Name(), 10, 1)
		if err != nil {
			logs.ReplicaLogger.Println(err)
			return nil, err
		}

		if objectOffset == fileOffset {
			targetOffset = fileOffset
			targetFound = true
			break
		} else if fileOffset > objectOffset {
			if minDistance > fileOffset-objectOffset {
				targetOffset = fileOffset
				minDistance = fileOffset - objectOffset
				targetFound = true
			}
		}
	}

	if !targetFound {
		return nil, ErrEmptyOffset
	}

	data, err := os.ReadFile(fmt.Sprintf("%s/%d", pathToKey, targetOffset))
	if err != nil {
		logs.ReplicaLogger.Println(err)
		return nil, err
	}

	packet := &internalTypes.PartPacket{}

	packet, err = packet.FromBytes(data)
	if err != nil {
		logs.ReplicaLogger.Println(err)
		return nil, err
	}

	return packet, nil
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
