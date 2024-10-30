package logs

import (
	"log"
	"os"
)

var MainLogger = log.New(os.Stdout, "main :: ", log.Ldate|log.Ltime)

var ReplicasLogger = log.New(os.Stdout, "replicas :: ", log.Ldate|log.Ltime)

var ReplicaLogger = log.New(os.Stdout, "replica :: ", log.Ldate|log.Ltime)
