package topology

type ShardConfig struct {
	Address   string `json:"address"`
	Port      uint16 `json:"port"`
	BucketIdx uint16 `json:"bidx"`
	KeyIdx    uint16 `json:"kidx"`
	ObjectIdx uint16 `json:"cidx"`
}

type ShardsTopologyConfig struct {
	ShardsConfigs []ShardConfig `json:"shards"`
}
