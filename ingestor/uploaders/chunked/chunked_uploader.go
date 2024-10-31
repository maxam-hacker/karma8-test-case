package chunked

import (
	ingestorApi "karma8-storage/api/ingestor"
	"karma8-storage/ingestor/shards"
	"karma8-storage/internals/types"
)

func UploadChunk(chunk *ingestorApi.Chunk) error {
	return shards.UploadChunk(&types.PartPacket{
		Bucket:          chunk.Bucket,
		Key:             chunk.Key,
		Data:            chunk.Data,
		Offset:          chunk.Offset,
		PacketSize:      chunk.ChunkSize,
		TotalObjectSize: chunk.TotalSize,
	})
}
