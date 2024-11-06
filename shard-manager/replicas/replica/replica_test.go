package replica

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"

	internalTypes "karma8-storage/internals/types"
)

func Test0001(t *testing.T) {
	basePath := "/tmp/karma8-tests/replica/test0"

	r1 := ShardReplica{
		BasePath: basePath,
	}
	err := os.MkdirAll(r1.BasePath, os.ModePerm)
	assert.NoError(t, err)

	part0Data := []byte("First packet data")
	part1Data := []byte("Second packet data")
	part2Data := []byte("Third packet data!!!")
	totalObjectSize := len(part0Data) + len(part1Data) + len(part2Data)

	part0 := internalTypes.ObjectPart{
		Bucket:            "karma8-test-case",
		Key:               "replica-test-case-0",
		Data:              &part0Data,
		PartDataSize:      uint64(len(part0Data)),
		TotalObjectOffset: 0,
		TotalObjectSize:   uint64(totalObjectSize),
		Opts:              internalTypes.ObjectPartOptions{},
	}
	err = r1.WriteObjectPart(part0)
	assert.NoError(t, err)

	part1 := internalTypes.ObjectPart{
		Bucket:            "karma8-test-case",
		Key:               "replica-test-case-0",
		Data:              &part0Data,
		PartDataSize:      uint64(len(part1Data)),
		TotalObjectOffset: uint64(len(part0Data)),
		TotalObjectSize:   uint64(totalObjectSize),
		Opts:              internalTypes.ObjectPartOptions{},
	}
	err = r1.WriteObjectPart(part1)
	assert.NoError(t, err)

	part2 := internalTypes.ObjectPart{
		Bucket:            "karma8-test-case",
		Key:               "replica-test-case-0",
		Data:              &part0Data,
		PartDataSize:      uint64(len(part2Data)),
		TotalObjectOffset: uint64(len(part0Data)) + uint64(len(part1Data)),
		TotalObjectSize:   uint64(totalObjectSize),
		Opts:              internalTypes.ObjectPartOptions{},
	}
	err = r1.WriteObjectPart(part2)
	assert.NoError(t, err)

	newPart0, err := r1.ReadObjectPart("karma8-test-case", "replica-test-case-0", 0)
	assert.NoError(t, err)
	assert.Equal(t, string(*part0.Data), string(*newPart0.Data))

	newPart1, err := r1.ReadObjectPart("karma8-test-case", "replica-test-case-0", newPart0.TotalObjectOffset)
	assert.NoError(t, err)
	assert.Equal(t, string(*part1.Data), string(*newPart1.Data))

	newPart2, err := r1.ReadObjectPart("karma8-test-case", "replica-test-case-0", newPart1.TotalObjectOffset)
	assert.NoError(t, err)
	assert.Equal(t, string(*part2.Data), string(*newPart2.Data))

	err = r1.DeleteReplica()
	assert.NoError(t, err)

	os.RemoveAll(basePath)
}
