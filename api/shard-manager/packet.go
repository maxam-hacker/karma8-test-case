package shardmanager

type PartPacketOptions struct {
	BucketShardsNumber uint16
	KeyShardsNumber    uint16
	ObjectShardsNumber uint16
}

type PartPacket struct {
	Bucket          string
	Key             string
	Data            []byte
	Offset          uint64
	PacketSize      uint64
	TotalObjectSize uint64
	Opts            PartPacketOptions
}
