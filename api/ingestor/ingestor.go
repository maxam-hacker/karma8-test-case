package ingestor

import (
	"encoding/json"
	"io"
)

type IngestorRequest struct {
	DownloadOpts      DownloadOptions
	UploadOpts        UploadOptions
	UploadChankedOpts UploadChankedOptions
}

type IngestorRequestReader struct {
	idx          int
	RequestBytes []byte
}

func (request *IngestorRequest) NewReader() (*IngestorRequestReader, error) {
	requestAsBytes, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}

	return &IngestorRequestReader{
		idx:          0,
		RequestBytes: requestAsBytes,
	}, nil
}

func (reader IngestorRequestReader) Read(buff []byte) (int, error) {
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

type IngestorResponse struct {
	Status string
	Error  string
}

func (response *IngestorResponse) FromBytes(bytes []byte) (*IngestorResponse, error) {
	err := json.Unmarshal(bytes, response)
	if err != nil {
		return nil, err
	}

	return response, nil
}
