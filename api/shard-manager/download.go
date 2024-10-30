package shardmanager

type DownloadOptions struct {
	Packets []PartPacket
}

func (opts *DownloadOptions) IsEmpty() bool {
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
