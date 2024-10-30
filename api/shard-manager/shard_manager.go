package shardmanager

import (
	"encoding/json"
	"io"
)

type ShardManagerRequest struct {
	UploadOpts   UploadOptions
	DownloadOpts DownloadOptions
}

type ShardManagerRequestReader struct {
	idx          int
	RequestBytes []byte
}

func (request *ShardManagerRequest) NewReader() (*ShardManagerRequestReader, error) {
	requestAsBytes, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}

	return &ShardManagerRequestReader{
		idx:          0,
		RequestBytes: requestAsBytes,
	}, nil
}

func (reader ShardManagerRequestReader) Read(buff []byte) (int, error) {
	if len(buff) == 0 {
		return 0, nil
	}

	idx0 := 0
	for ; idx0 < len(buff); idx0++ {
		buff[idx0] = reader.RequestBytes[reader.idx]

		reader.idx++

		if reader.idx >= len(reader.RequestBytes) {
			return idx0 + 1, io.EOF
		}
	}

	return idx0, nil
}

type ShardManagerResponse struct {
	Packets []PartPacket
}

func (response *ShardManagerResponse) FromBytes(bytes []byte) (*ShardManagerResponse, error) {
	err := json.Unmarshal(bytes, response)
	if err != nil {
		return nil, err
	}

	return response, nil
}
