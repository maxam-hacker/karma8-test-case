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

	ingestorApi "karma8-storage/api/ingestor"
	"karma8-storage/ingestor/logs"
	"karma8-storage/ingestor/shards"
	"karma8-storage/ingestor/uploaders/chunked"
	"karma8-storage/internals/rest"
)

var (
	ErrUnknownOpts = errors.New("unknown request options")
)

func process(ingestorRequest *ingestorApi.IngestorRequest) (*ingestorApi.IngestorResponse, error) {
	logs.MainLogger.Println("process request...")

	response := &ingestorApi.IngestorResponse{}

	if !ingestorRequest.UploadOpts.IsEmpty() {
		logs.MainLogger.Println(ErrUnknownOpts)
		return response, ErrUnknownOpts
	}

	if !ingestorRequest.UploadChankedOpts.IsEmpty() {
		logs.MainLogger.Println("chanked uploading...")

		err := chunked.UploadOneChunk(&ingestorRequest.UploadChankedOpts.Chunk)
		if err != nil {
			logs.MainLogger.Println(err)
			response.Status = "error"
			response.Error = "can't upload chunk"
			return response, err
		}

		response.Status = "ok"
		return response, nil
	}

	logs.MainLogger.Println(ErrUnknownOpts)

	return response, ErrUnknownOpts
}

func do(w http.ResponseWriter, r *http.Request) {
	logs.MainLogger.Println("do request...")

	serviceRequestBody, err := io.ReadAll(r.Body)
	if err != nil {
		w.Header().Add("X-Karma8-Ingestor-Service-Error", "can't read service request")
		w.Header().Add("X-Karma8-Ingestor-Service-Error-Content", err.Error())
		w.WriteHeader(200)
		logs.MainLogger.Println(err)
		return
	}

	var ingestorRequest ingestorApi.IngestorRequest

	err = json.Unmarshal(serviceRequestBody, &ingestorRequest)
	if err != nil {
		w.Header().Add("X-Karma8-Ingestor-Service-Error", "can't unmarshal service request")
		w.Header().Add("X-Karma8-Ingestor-Service-Error-Content", err.Error())
		w.WriteHeader(200)
		logs.MainLogger.Println(err)
		return
	}

	ingestorResponse, err := process(&ingestorRequest)
	if err != nil {
		w.Header().Add("X-Karma8-Ingestor-Service-Error", "can't process request")
		w.Header().Add("X-Karma8-Ingestor-Service-Error-Content", err.Error())
		w.WriteHeader(200)
		logs.MainLogger.Println(err)
		return
	}

	responseContent, err := json.Marshal(ingestorResponse)
	if err != nil {
		w.Header().Add("X-Hashtag-Ingestor-Service-Error", "can't marshal service response")
		w.Header().Add("X-Hashtag-Ingestor-Service-Error-Content", err.Error())
		w.WriteHeader(200)
		logs.MainLogger.Println(err)
		return
	}

	w.Header().Add("Content-Type", "text/json")
	w.Write(responseContent)
}

func runRestServer() {
	logs.MainLogger.Println("REST server...")

	targetServiceAddr := os.Getenv("INGESTOR_SERVICE_ADDR")
	targetServicePort := os.Getenv("INGESTOR_SERVICE_PORT")

	router := chi.NewRouter()

	router.Post("/ingestor", do)

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
