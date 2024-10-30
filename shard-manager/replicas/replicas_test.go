package replicas

import (
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"

	internalTypes "karma8-storage/internals/types"
)

func Test0001(t *testing.T) {
	//launchReplicas("/tmp/karma8/test0/r1;/tmp/karma8/test0/r2;/tmp/karma8/test0/r3;/tmp/karma8/test0/r4")

	r1File, err := os.Stat("/tmp/karma8/test0/r1")
	assert.NoError(t, err)
	assert.Equal(t, "r1", r1File.Name())

	r2File, err := os.Stat("/tmp/karma8/test0/r2")
	assert.NoError(t, err)
	assert.Equal(t, "r2", r2File.Name())

	r3File, err := os.Stat("/tmp/karma8/test0/r3")
	assert.NoError(t, err)
	assert.Equal(t, "r3", r3File.Name())

	r4File, err := os.Stat("/tmp/karma8/test0/r4")
	assert.NoError(t, err)
	assert.Equal(t, "r4", r4File.Name())

	deleteReplicas()

	os.RemoveAll("/tmp/karma8/test0")
}

func Test0002(t *testing.T) {
	//launchReplicas("/tmp/karma8/test1/r1;/tmp/karma8/test1/r2;/tmp/karma8/test1/r3;/tmp/karma8/test1/r4")

	packetData := []byte("Replica test1 data smple")
	totalObjectSize := len(packetData)

	packetBucket := "karma8-test-case-0-bucket"
	packetKey := "replica-test-case-0-key"

	packet := &internalTypes.PartPacket{
		Bucket:          packetBucket,
		Key:             packetKey,
		Data:            packetData,
		Offset:          0,
		PacketSize:      uint64(len(packetData)),
		TotalObjectSize: uint64(totalObjectSize),
		Opts:            internalTypes.PartPacketOptions{},
	}

	err := WritePacket(packet)
	assert.NoError(t, err)

	r1File, err := os.Stat(path.Join("/tmp/karma8/test1/r1", packetBucket, packetKey, "0.pkt"))
	assert.NoError(t, err)
	assert.Equal(t, "0.pkt", r1File.Name())

	r2File, err := os.Stat(path.Join("/tmp/karma8/test1/r2", packetBucket, packetKey, "0.pkt"))
	assert.NoError(t, err)
	assert.Equal(t, "0.pkt", r2File.Name())

	r3File, err := os.Stat(path.Join("/tmp/karma8/test1/r3", packetBucket, packetKey, "0.pkt"))
	assert.NoError(t, err)
	assert.Equal(t, "0.pkt", r3File.Name())

	r4File, err := os.Stat(path.Join("/tmp/karma8/test1/r4", packetBucket, packetKey, "0.pkt"))
	assert.NoError(t, err)
	assert.Equal(t, "0.pkt", r4File.Name())

	os.RemoveAll("/tmp/karma8/test1")
}

func Test0003(t *testing.T) {
	//launchReplicas("/tmp/karma8/test2/r1;/tmp/karma8/test2/r2;/tmp/karma8/test2/r3;/tmp/karma8/test2/r4")

	packetData := []byte("Replica test2 data smple")
	totalObjectSize := len(packetData)

	packetBucket := "karma8-test-case-1-bucket"
	packetKey := "replica-test-case-1-key"

	packet := &internalTypes.PartPacket{
		Bucket:          packetBucket,
		Key:             packetKey,
		Data:            packetData,
		Offset:          0,
		PacketSize:      uint64(len(packetData)),
		TotalObjectSize: uint64(totalObjectSize),
		Opts:            internalTypes.PartPacketOptions{},
	}

	err := WritePacket(packet)
	assert.NoError(t, err)

	packet, err = ReadPacket(packetBucket, packetKey, 0)
	assert.NoError(t, err)
	assert.NotNil(t, packet)
	assert.Equal(t, packetData, packet.Data)

	os.RemoveAll("/tmp/karma8/test2")
}
