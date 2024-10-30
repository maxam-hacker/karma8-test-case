package ingestor

type Chunk struct {
	Bucket    string
	Key       string
	Offset    uint64
	Data      []byte
	ChunkSize uint64
	TotalSize uint64
}
