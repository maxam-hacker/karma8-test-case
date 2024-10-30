package logs

import (
	"log"
	"os"
)

var MainLogger = log.New(os.Stdout, "main :: ", log.Ldate|log.Ltime)

var ShardsLogger = log.New(os.Stdout, "shards :: ", log.Ldate|log.Ltime)

var ShardLogger = log.New(os.Stdout, "shard :: ", log.Ldate|log.Ltime)

var TopologyLogger = log.New(os.Stdout, "topology :: ", log.Ldate|log.Ltime)

var ChunkedUploadLogger = log.New(os.Stdout, "chunked upload :: ", log.Ldate|log.Ltime)

var SimpleUploadLogger = log.New(os.Stdout, "simple upload :: ", log.Ldate|log.Ltime)
