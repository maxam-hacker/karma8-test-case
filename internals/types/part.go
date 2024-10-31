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

func (part *ObjectPart) GetBytes() ([]byte, error) {
	bytes, err := json.Marshal(part)
	if err != nil {
		return nil, err
	}

	return bytes, nil
}
