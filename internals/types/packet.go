package types

import "encoding/json"

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

func (packet *PartPacket) GetBytes() ([]byte, error) {
	data, err := json.Marshal(packet)
	if err != nil {
		return nil, err
	}

	return data, nil
}

func (packet *PartPacket) FromBytes(bytes []byte) (*PartPacket, error) {
	err := json.Unmarshal(bytes, packet)
	if err != nil {
		return nil, err
	}

	return packet, nil
}
