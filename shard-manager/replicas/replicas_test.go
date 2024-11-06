package replicas

import (
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"

	internalTypes "karma8-storage/internals/types"
)

func TestMain(m *testing.M) {
	os.Setenv("REPLICAS_BASE_PATH", "/tmp/karma8-tests")
	os.Setenv("REPLICAS_INDEX", "123")
	os.Setenv("REPLICAS_PATHS", "test3-4-5/r1;/test3-4-5/r2;/test3-4-5/r3;/test3-4-5/r4")
	Initialize()

	m.Run()

	deleteReplicas()

	os.RemoveAll("/tmp/karma8-tests")

	os.Exit(0)
}

func Test0001(t *testing.T) {
	r1File, err := os.Stat("/tmp/karma8-tests/123/test3-4-5/r1")
	assert.NoError(t, err)
	assert.Equal(t, "r1", r1File.Name())

	r2File, err := os.Stat("/tmp/karma8-tests/123/test3-4-5/r2")
	assert.NoError(t, err)
	assert.Equal(t, "r2", r2File.Name())

	r3File, err := os.Stat("/tmp/karma8-tests/123/test3-4-5/r3")
	assert.NoError(t, err)
	assert.Equal(t, "r3", r3File.Name())

	r4File, err := os.Stat("/tmp/karma8-tests/123/test3-4-5/r4")
	assert.NoError(t, err)
	assert.Equal(t, "r4", r4File.Name())
}

func Test0002(t *testing.T) {
	partData := []byte("Replica test1 data smple")
	totalObjectSize := len(partData)

	partBucket := "karma8-test-case-0-bucket"
	partKey := "replica-test-case-0-key"

	part := internalTypes.ObjectPart{
		Bucket:            partBucket,
		Key:               partKey,
		Data:              &partData,
		PartDataSize:      uint64(len(partData)),
		TotalObjectOffset: 0,
		TotalObjectSize:   uint64(totalObjectSize),
		Opts:              internalTypes.ObjectPartOptions{},
	}

	err := WriteObjectPart(part)
	assert.NoError(t, err)

	r1File, err := os.Stat(path.Join("/tmp/karma8-tests/123/test3-4-5/r1", partBucket, partKey, "0"))
	assert.NoError(t, err)
	assert.Equal(t, "0", r1File.Name())

	r1MetaFile, err := os.Stat(path.Join("/tmp/karma8-tests/123/test3-4-5/r1", partBucket, partKey, "0.meta"))
	assert.NoError(t, err)
	assert.Equal(t, "0.meta", r1MetaFile.Name())

	r2File, err := os.Stat(path.Join("/tmp/karma8-tests/123/test3-4-5/r2", partBucket, partKey, "0"))
	assert.NoError(t, err)
	assert.Equal(t, "0", r2File.Name())

	r2MetaFile, err := os.Stat(path.Join("/tmp/karma8-tests/123/test3-4-5/r2", partBucket, partKey, "0.meta"))
	assert.NoError(t, err)
	assert.Equal(t, "0.meta", r2MetaFile.Name())

	r3File, err := os.Stat(path.Join("/tmp/karma8-tests/123/test3-4-5/r3", partBucket, partKey, "0"))
	assert.NoError(t, err)
	assert.Equal(t, "0", r3File.Name())

	r3MetaFile, err := os.Stat(path.Join("/tmp/karma8-tests/123/test3-4-5/r3", partBucket, partKey, "0.meta"))
	assert.NoError(t, err)
	assert.Equal(t, "0.meta", r3MetaFile.Name())

	r4File, err := os.Stat(path.Join("/tmp/karma8-tests/123/test3-4-5/r4", partBucket, partKey, "0"))
	assert.NoError(t, err)
	assert.Equal(t, "0", r4File.Name())

	r4MetaFile, err := os.Stat(path.Join("/tmp/karma8-tests/123/test3-4-5/r4", partBucket, partKey, "0.meta"))
	assert.NoError(t, err)
	assert.Equal(t, "0.meta", r4MetaFile.Name())
}

func Test0003(t *testing.T) {
	partData := []byte("Replica test1 data smple")
	totalObjectSize := len(partData)

	partBucket := "karma8-test-case-0-bucket"
	partKey := "replica-test-case-0-key"

	part := internalTypes.ObjectPart{
		Bucket:            partBucket,
		Key:               partKey,
		Data:              &partData,
		PartDataSize:      uint64(len(partData)),
		TotalObjectOffset: 0,
		TotalObjectSize:   uint64(totalObjectSize),
		Opts:              internalTypes.ObjectPartOptions{},
	}

	err := WriteObjectPart(part)
	assert.NoError(t, err)

	existingPart, err := ReadObjectPart(partBucket, partKey, 0)
	assert.NoError(t, err)
	assert.NotNil(t, existingPart)
	assert.Equal(t, partData, *existingPart.Data)
}
