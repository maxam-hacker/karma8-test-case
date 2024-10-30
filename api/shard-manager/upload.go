package shardmanager

type UploadOptions struct {
	Packets []PartPacket
}

func (opts *UploadOptions) IsEmpty() bool {
	if len(opts.Packets) == 0 {
		return true
	}

	for _, packet := range opts.Packets {
		if packet.Bucket == "" || packet.Key == "" {
			return true
		}
	}

	return false
}
