package main

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/go-chi/chi"

	"karma8-storage/ingestor/logs"
	"karma8-storage/ingestor/shards"
	"karma8-storage/internals/rest"
	internalTypes "karma8-storage/internals/types"
)

var (
	ErrUnknownOpts = errors.New("unknown request options")
)

func doUpload(w http.ResponseWriter, r *http.Request) {
	objectBucket := r.Header.Get("X-Karma8-Object-Bucket")
	objectKey := r.Header.Get("X-Karma8-Object-Key")
	objectTotalSize := r.Header.Get("X-Karma8-Object-Total-Size")

	logs.MainLogger.Println("uploading object...")
	logs.MainLogger.Println("bucket:", objectBucket)
	logs.MainLogger.Println("key", objectKey)
	logs.MainLogger.Println("total size", objectTotalSize)

	bytesBuffer := make([]byte, 16*1024)

	totalSize, err := strconv.Atoi(objectTotalSize)
	if err != nil {
		w.Header().Add("X-Karma8-Ingestor-Service-Error", "error while uploading file")
		w.Header().Add("X-Karma8-Ingestor-Service-Error-Content", err.Error())
		w.WriteHeader(200)
		logs.MainLogger.Println(err)
		return
	}

	totalSizeProcessed := 0

	totalOffset := 0
	partDataBuffer := make([]byte, 0)
	doRead := true

	for doRead {
		n, err := r.Body.Read(bytesBuffer)
		if err != nil {
			if err == io.EOF {
				doRead = false
			} else {
				w.Header().Add("X-Karma8-Ingestor-Service-Error", "error while uploading file")
				w.Header().Add("X-Karma8-Ingestor-Service-Error-Content", err.Error())
				w.WriteHeader(200)
				logs.MainLogger.Println(err)
				return
			}
		}

		partDataBuffer = append(partDataBuffer, bytesBuffer[0:n]...)
		if len(partDataBuffer) >= 10*1024*1024 {
			shards.UploadPart(internalTypes.ObjectPart{
				Bucket:            objectBucket,
				Key:               objectKey,
				Data:              &partDataBuffer,
				PartDataSize:      uint64(len(partDataBuffer)),
				TotalObjectOffset: uint64(totalOffset),
				TotalObjectSize:   uint64(totalSize),
			})

			totalOffset += len(partDataBuffer)
			partDataBuffer = make([]byte, 0)
		}

		totalSizeProcessed += n
	}

	if len(partDataBuffer) > 0 {
		shards.UploadPart(internalTypes.ObjectPart{
			Bucket:            objectBucket,
			Key:               objectKey,
			Data:              &partDataBuffer,
			PartDataSize:      uint64(len(partDataBuffer)),
			TotalObjectOffset: uint64(totalOffset),
			TotalObjectSize:   uint64(totalSize),
		})
	}

	logs.MainLogger.Println("done", totalSizeProcessed)
}

func doDownload(w http.ResponseWriter, r *http.Request) {
}

func runRestServer() {
	logs.MainLogger.Println("REST server...")

	targetServiceAddr := os.Getenv("INGESTOR_SERVICE_ADDR")
	targetServicePort := os.Getenv("INGESTOR_SERVICE_PORT")

	router := chi.NewRouter()

	router.Post("/ingestor/file/upload", doUpload)
	router.Post("/ingestor/file/download", doDownload)

	rest.NewHttpServer(
		fmt.Sprintf("%s:%s", targetServiceAddr, targetServicePort),
		router,
	).Start()
}

func main() {
	logs.MainLogger.Println("Starting...")

	shards.Initialize(os.Getenv("INGESTOR_SERVICE_SHARDS_TOPOLOGY_CONFIG"))

	go runRestServer()

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	<-sigs
}
