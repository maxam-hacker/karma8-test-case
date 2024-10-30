package replica

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"

	internalTypes "karma8-storage/internals/types"
)

func Test0001(t *testing.T) {
	r1 := ShardReplica{
		BasePath: "/tmp/karma8/replica/test0",
	}
	err := os.MkdirAll(r1.BasePath, os.ModePerm)
	assert.NoError(t, err)

	packet0Data := []byte("First packet data")
	packet1Data := []byte("Second packet data")
	packet2Data := []byte("Third packet data")
	totalObjectSize := len(packet0Data) + len(packet1Data) + len(packet2Data)

	packet0 := &internalTypes.PartPacket{
		Bucket:          "karma8-test-case",
		Key:             "replica-test-case-0",
		Data:            packet0Data,
		Offset:          0,
		PacketSize:      uint64(len(packet0Data)),
		TotalObjectSize: uint64(totalObjectSize),
		Opts:            internalTypes.PartPacketOptions{},
	}
	err = r1.WritePacket(packet0)
	assert.NoError(t, err)

	packet1 := &internalTypes.PartPacket{
		Bucket:          "karma8-test-case",
		Key:             "replica-test-case-0",
		Data:            packet1Data,
		Offset:          uint64(len(packet0Data)),
		PacketSize:      uint64(len(packet1Data)),
		TotalObjectSize: uint64(totalObjectSize),
		Opts:            internalTypes.PartPacketOptions{},
	}
	err = r1.WritePacket(packet1)
	assert.NoError(t, err)

	packet2 := &internalTypes.PartPacket{
		Bucket:          "karma8-test-case",
		Key:             "replica-test-case-0",
		Data:            packet2Data,
		Offset:          uint64(len(packet0Data)) + uint64(len(packet1Data)),
		PacketSize:      uint64(len(packet2Data)),
		TotalObjectSize: uint64(totalObjectSize),
		Opts:            internalTypes.PartPacketOptions{},
	}
	err = r1.WritePacket(packet2)
	assert.NoError(t, err)

	newPacket0, err := r1.ReadPacket("karma8-test-case", "replica-test-case-0", 0)
	assert.NoError(t, err)
	assert.Equal(t, "First packet data", string(newPacket0.Data))

	newPacket1, err := r1.ReadPacket("karma8-test-case", "replica-test-case-0", uint64(len(newPacket0.Data)))
	assert.NoError(t, err)
	assert.Equal(t, "Second packet data", string(newPacket1.Data))

	newPacket2, err := r1.ReadPacket("karma8-test-case", "replica-test-case-0", uint64(len(newPacket0.Data)+len(newPacket1.Data)))
	assert.NoError(t, err)
	assert.Equal(t, "Third packet data", string(newPacket2.Data))

	err = r1.DeleteReplica()
	assert.NoError(t, err)

	os.RemoveAll("/tmp/karma8/replica/test0")
}
