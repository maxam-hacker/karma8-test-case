package types

import "encoding/json"

type ObjectPartOptions struct {
	BucketShardsNumber uint16
	KeyShardsNumber    uint16
	ObjectShardsNumber uint16
}

type ObjectPart struct {
	Bucket            string
	Key               string
	Data              *[]byte
	PartDataSize      uint64
	TotalObjectOffset uint64
	TotalObjectSize   uint64
	Opts              ObjectPartOptions
}

type ObjectPartMeta struct {
	Bucket            string
	Key               string
	PartDataSize      uint64
	TotalObjectOffset uint64
	TotalObjectSize   uint64
	Opts              ObjectPartOptions
	Arg0              any
}

func (part *ObjectPart) GetBytes() ([]byte, error) {
	bytes, err := json.Marshal(part)
	if err != nil {
		return nil, err
	}

	return bytes, nil
}

func (part *ObjectPart) GetMetaBytes() ([]byte, error) {
	objectPartMeta := ObjectPartMeta{
		Bucket:            part.Bucket,
		Key:               part.Key,
		PartDataSize:      part.PartDataSize,
		TotalObjectOffset: part.TotalObjectOffset,
		TotalObjectSize:   part.TotalObjectSize,
		Opts:              part.Opts,
	}

	bytes, err := json.Marshal(objectPartMeta)
	if err != nil {
		return nil, err
	}

	return bytes, nil
}
