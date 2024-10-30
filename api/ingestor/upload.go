package ingestor

type UploadOptions struct {
	Bucket string
	Key    string
}

func (opts *UploadOptions) IsEmpty() bool {
	if opts.Bucket == "" || opts.Key == "" {
		return true
	}

	return false
}

type UploadChankedOptions struct {
	Chunk Chunk
}

func (opts *UploadChankedOptions) IsEmpty() bool {
	if opts.Chunk.Bucket == "" || opts.Chunk.Key == "" {
		return true
	}

	return false
}
