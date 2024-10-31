package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/go-chi/chi"

	shardManagerApi "karma8-storage/api/shard-manager"
	"karma8-storage/internals/rest"
	internalTypes "karma8-storage/internals/types"
	"karma8-storage/shard-manager/logs"
	"karma8-storage/shard-manager/replicas"
)

var (
	ErrUnknownOpts = errors.New("unknown request options")
)

func uploadPackage(opts *shardManagerApi.UploadOptions) (*shardManagerApi.ShardManagerResponse, error) {
	logs.MainLogger.Println("upload package...", opts)

	response := &shardManagerApi.ShardManagerResponse{
		Packets: make([]shardManagerApi.PartPacket, 0),
	}

	for _, shardPacketInfo := range opts.Packets {
		err := replicas.WritePacket(&internalTypes.PartPacket{
			Bucket:          shardPacketInfo.Bucket,
			Key:             shardPacketInfo.Key,
			Data:            shardPacketInfo.Data,
			Offset:          shardPacketInfo.Offset,
			PacketSize:      shardPacketInfo.PacketSize,
			TotalObjectSize: shardPacketInfo.TotalObjectSize,
			Opts: internalTypes.PartPacketOptions{
				BucketShardsNumber: shardPacketInfo.Opts.BucketShardsNumber,
				KeyShardsNumber:    shardPacketInfo.Opts.KeyShardsNumber,
				ObjectShardsNumber: shardPacketInfo.Opts.ObjectShardsNumber,
			},
		})
		if err != nil {
			logs.MainLogger.Println(err)
			continue
		}

		response.Packets = append(response.Packets, shardManagerApi.PartPacket{
			Bucket: shardPacketInfo.Bucket,
			Key:    shardPacketInfo.Key,
			Offset: shardPacketInfo.Offset,
		})
	}

	return response, nil
}

func downloadPackage(opts *shardManagerApi.DownloadOptions) (*shardManagerApi.ShardManagerResponse, error) {
	logs.MainLogger.Println("download package...", opts)

	response := &shardManagerApi.ShardManagerResponse{
		Packets: make([]shardManagerApi.PartPacket, 0),
	}

	for _, shardPacketInfo := range opts.Packets {
		storagePacket, err := replicas.ReadPacket(shardPacketInfo.Bucket, shardPacketInfo.Key, shardPacketInfo.Offset)
		if err != nil {
			logs.MainLogger.Println(err)
			continue
		}

		response.Packets = append(response.Packets, shardManagerApi.PartPacket{
			Bucket:          storagePacket.Bucket,
			Key:             storagePacket.Key,
			Data:            storagePacket.Data,
			Offset:          storagePacket.Offset,
			PacketSize:      storagePacket.PacketSize,
			TotalObjectSize: storagePacket.TotalObjectSize,
			Opts: shardManagerApi.PartPacketOptions{
				BucketShardsNumber: storagePacket.Opts.BucketShardsNumber,
				KeyShardsNumber:    storagePacket.Opts.KeyShardsNumber,
				ObjectShardsNumber: storagePacket.Opts.ObjectShardsNumber,
			},
		})
	}

	return response, nil
}

func process(shardManagerRequest *shardManagerApi.ShardManagerRequest) (*shardManagerApi.ShardManagerResponse, error) {
	logs.MainLogger.Println("process request...", shardManagerRequest)

	if !shardManagerRequest.UploadOpts.IsEmpty() {
		return uploadPackage(&shardManagerRequest.UploadOpts)
	}
	if !shardManagerRequest.DownloadOpts.IsEmpty() {
		return downloadPackage(&shardManagerRequest.DownloadOpts)
	}

	logs.MainLogger.Println("unknown options")

	return nil, ErrUnknownOpts
}

func do(w http.ResponseWriter, r *http.Request) {
	logs.MainLogger.Println("do request...", r)

	reader := io.LimitReader(r.Body, 10*1024*1024)

	serviceRequestBody := make([]byte, 10*1024*1024)

	n, err := reader.Read(serviceRequestBody)
	if err != nil {
		w.Header().Add("X-Karma8-Shard-Manager-Service-Error", "can't read service request")
		w.Header().Add("X-Karma8-Shard-Manager-Service-Error-Content", err.Error())
		w.WriteHeader(200)
		logs.MainLogger.Println(err)
		return
	}

	fmt.Println(n)

	var shardManagerRequest shardManagerApi.ShardManagerRequest

	err = json.Unmarshal(serviceRequestBody[0:n], &shardManagerRequest)
	if err != nil {
		w.Header().Add("X-Karma8-Shard-Manager-Service-Error", "can't unmarshal service request")
		w.Header().Add("X-Karma8-Shard-Manager-Service-Error-Content", err.Error())
		w.WriteHeader(200)
		logs.MainLogger.Println(err)
		return
	}

	shardManagerResponse, err := process(&shardManagerRequest)
	if err != nil {
		w.Header().Add("X-Karma8-Shard-Manager-Service-Error", "can't process request")
		w.Header().Add("X-Karma8-Shard-Manager-Service-Error-Content", err.Error())
		w.WriteHeader(200)
		logs.MainLogger.Println(err)
		return
	}

	responseContent, err := json.Marshal(shardManagerResponse)
	if err != nil {
		w.Header().Add("X-Hashtag-Shard-Manager-Service-Error", "can't marshal service response")
		w.Header().Add("X-Hashtag-Shard-Manager-Service-Error-Content", err.Error())
		w.WriteHeader(200)
		logs.MainLogger.Println(err)
		return
	}

	w.Header().Add("Content-Type", "text/json")
	w.Write(responseContent)
}

func runRestServer() {
	logs.MainLogger.Println("RESR server...")

	targetServiceAddr := os.Getenv("SHARD_MANAGER_SERVICE_ADDR")
	targetServicePort := os.Getenv("SHARD_MANAGER_SERVICE_PORT")

	logs.MainLogger.Println("address", targetServiceAddr)
	logs.MainLogger.Println("port", targetServicePort)

	router := chi.NewRouter()

	router.Post("/shard-manager", do)

	rest.NewHttpServer(
		fmt.Sprintf("%s:%s", targetServiceAddr, targetServicePort),
		router,
	).Start()
}

func main() {
	logs.MainLogger.Println("Starting...")

	replicas.Initialize()

	go runRestServer()

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	<-sigs
}
