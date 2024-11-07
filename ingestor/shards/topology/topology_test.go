package topology

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test0001(t *testing.T) {
	storage, err := Create("../../topology.config")
	assert.NoError(t, err)
	assert.NotNil(t, storage)

	assert.Equal(t, len(storage.BucketsShards), 1)
	assert.Equal(t, int(storage.BucketsShardsCount), 1)

	assert.Equal(t, len(storage.BucketsShards[0].KeysShards), 1)
	assert.Equal(t, int(storage.BucketsShards[0].KeysShardsCount), 1)

	assert.Equal(t, len(storage.BucketsShards[0].KeysShards[0].ObjectsShards), 8)
	assert.Equal(t, int(storage.BucketsShards[0].KeysShards[0].ObjectsShardsCount), 8)
}
