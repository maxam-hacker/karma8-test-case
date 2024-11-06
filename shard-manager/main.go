package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/go-chi/chi"

	"karma8-storage/internals/rest"
	internalTypes "karma8-storage/internals/types"
	internalUtils "karma8-storage/internals/utils"
	"karma8-storage/shard-manager/logs"
	"karma8-storage/shard-manager/replicas"
)

var (
	ServiceErrorHeader        = "X-Karma8-Shard-Manager-Service-Error"
	ServiceErrorContentHeader = "X-Karma8-Shard-Manager-Service-Error-Content"
)

func doMeta(w http.ResponseWriter, r *http.Request) {
	logs.MainLogger.Println("do meta request...")

	objectBucket, err := internalUtils.ObjectBucketGetAndValidate(r)
	if err != nil {
		w.Header().Add(ServiceErrorHeader, "can't get object bucket name")
		w.Header().Add(ServiceErrorContentHeader, err.Error())
		w.WriteHeader(404)
		logs.MainLogger.Println(err)
		return
	}

	objectKey, err := internalUtils.ObjectKeyGetAndValidate(r)
	if err != nil {
		w.Header().Add(ServiceErrorHeader, "can't get object key value")
		w.Header().Add(ServiceErrorContentHeader, err.Error())
		w.WriteHeader(404)
		logs.MainLogger.Println(err)
		return
	}

	logs.MainLogger.Println("meta for object...")
	logs.MainLogger.Println("bucket:", objectBucket)
	logs.MainLogger.Println("key:", objectKey)

	partsMeta, err := replicas.ReadObjectPartsMeta(objectBucket, objectKey)
	if err != nil {
		w.Header().Add(ServiceErrorHeader, "error while reading object parts meta")
		w.Header().Add(ServiceErrorContentHeader, err.Error())
		w.WriteHeader(500)
		logs.MainLogger.Println(err)
		return
	}

	partsMetaBytes, err := json.Marshal(partsMeta)
	if err != nil {
		w.Header().Add(ServiceErrorHeader, "error while marshal object parts meta")
		w.Header().Add(ServiceErrorContentHeader, err.Error())
		w.WriteHeader(500)
		logs.MainLogger.Println(err)
		return
	}

	w.Header().Add("Content-Type", "text/json")
	w.Write(partsMetaBytes)
}

func doDownload(w http.ResponseWriter, r *http.Request) {
	logs.MainLogger.Println("do download request...")

	objectBucket := r.Header.Get("X-Karma8-Object-Bucket")
	objectKey := r.Header.Get("X-Karma8-Object-Key")
	totalObjectOffset := r.Header.Get("X-Karma8-Object-Total-Offset")

	logs.MainLogger.Println("download object part...")
	logs.MainLogger.Println("bucket:", objectBucket)
	logs.MainLogger.Println("key:", objectKey)

	offset, err := strconv.ParseUint(totalObjectOffset, 10, 0)
	if err != nil {
		w.Header().Add(ServiceErrorHeader, "error while parsing total object offset")
		w.Header().Add(ServiceErrorContentHeader, err.Error())
		w.WriteHeader(200)
		logs.MainLogger.Println(err)
		return
	}

	objectPart, err := replicas.ReadObjectPart(objectBucket, objectKey, offset)
	if err != nil {
		w.Header().Add(ServiceErrorHeader, "error while reading object part")
		w.Header().Add(ServiceErrorContentHeader, err.Error())
		w.WriteHeader(200)
		logs.MainLogger.Println(err)
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Write(*objectPart.Data)
}

func doUpload(w http.ResponseWriter, r *http.Request) {
	logs.MainLogger.Println("do upload request...")

	objectBucket := r.Header.Get("X-Karma8-Object-Bucket")
	objectKey := r.Header.Get("X-Karma8-Object-Key")
	objectPartDataSize := r.Header.Get("X-Karma8-Object-Part-Data-Size")
	objectTotalOffset := r.Header.Get("X-Karma8-Object-Total-Offset")
	objectTotalSize := r.Header.Get("X-Karma8-Object-Total-Size")

	logs.MainLogger.Println("uploading object...")
	logs.MainLogger.Println("bucket:", objectBucket)
	logs.MainLogger.Println("key", objectKey)
	logs.MainLogger.Println("part data size", objectPartDataSize)
	logs.MainLogger.Println("total offset", objectTotalOffset)
	logs.MainLogger.Println("total size", objectTotalSize)

	partSize, err := strconv.ParseUint(objectPartDataSize, 10, 0)
	if err != nil {
		w.Header().Add(ServiceErrorHeader, "error while uploading file")
		w.Header().Add(ServiceErrorContentHeader, err.Error())
		w.WriteHeader(404)
		logs.MainLogger.Println(err)
		return
	}

	totalOffset, err := strconv.ParseUint(objectTotalOffset, 10, 0)
	if err != nil {
		w.Header().Add(ServiceErrorHeader, "error while uploading file")
		w.Header().Add(ServiceErrorContentHeader, err.Error())
		w.WriteHeader(404)
		logs.MainLogger.Println(err)
		return
	}

	totalSize, err := strconv.ParseUint(objectTotalSize, 10, 0)
	if err != nil {
		w.Header().Add(ServiceErrorHeader, "error while uploading file")
		w.Header().Add(ServiceErrorContentHeader, err.Error())
		w.WriteHeader(404)
		logs.MainLogger.Println(err)
		return
	}

	bytesBuffer := make([]byte, 16*1024)
	partDataBuffer := make([]byte, 0)

	totalSizeProcessed := 0
	doRead := true

	for doRead {
		n, err := r.Body.Read(bytesBuffer)
		if err != nil {
			if err == io.EOF {
				doRead = false
			} else {
				w.Header().Add(ServiceErrorHeader, "error while uploading file")
				w.Header().Add(ServiceErrorContentHeader, err.Error())
				w.WriteHeader(500)
				logs.MainLogger.Println(err)
				return
			}
		}

		partDataBuffer = append(partDataBuffer, bytesBuffer[0:n]...)
		totalSizeProcessed += n
	}

	err = replicas.WriteObjectPart(internalTypes.ObjectPart{
		Bucket:            objectBucket,
		Key:               objectKey,
		Data:              &partDataBuffer,
		PartDataSize:      partSize,
		TotalObjectOffset: totalOffset,
		TotalObjectSize:   totalSize,
	})

	if err != nil {
		w.Header().Add(ServiceErrorHeader, "error while writing object part")
		w.Header().Add(ServiceErrorContentHeader, err.Error())
		w.WriteHeader(500)
		logs.MainLogger.Println(err)
		return
	}

	logs.MainLogger.Println("done", totalSize, totalSizeProcessed)
}

func runRestServer() {
	logs.MainLogger.Println("REST server...")

	targetServiceAddr := os.Getenv("SHARD_MANAGER_SERVICE_ADDR")
	targetServicePort := os.Getenv("SHARD_MANAGER_SERVICE_PORT")

	logs.MainLogger.Println("address", targetServiceAddr)
	logs.MainLogger.Println("port", targetServicePort)

	router := chi.NewRouter()

	router.Post("/shard-manager/object/part/upload", doUpload)
	router.Post("/shard-manager/object/part/download", doDownload)
	router.Post("/shard-manager/object/meta", doMeta)

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
