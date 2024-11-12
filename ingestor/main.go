package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-chi/chi"
	"github.com/go-chi/cors"
	httpSwagger "github.com/swaggo/http-swagger/v2"

	_ "karma8-storage/api"
	"karma8-storage/ingestor/logs"
	"karma8-storage/ingestor/shards"
	"karma8-storage/internals/rest"
	internalTypes "karma8-storage/internals/types"
	internalUtils "karma8-storage/internals/utils"
)

var (
	ServiceErrorHeader        = "X-Karma8-Ingestor-Service-Error"
	ServiceErrorContentHeader = "X-Karma8-Ingestor-Service-Error-Content"
	ObjectPartSize            = 10 * 1024 * 1024

	debugService string
)

// @Summary		Upload file
//
// @Description	Upload file with bucket name and key value
// @Accept		octet-stream
// @Param		X-Karma8-Object-Bucket			header			string 	true	"Bucket name for target file"
// @Param		X-Karma8-Object-Key				header			string 	true	"Key value for target file"
// @Param		X-Karma8-Object-Total-Size		header			string 	true	"Total size of target file"
// @Param		file							formData		file	true	"Target file"
// @Success		200
// @Router		/ingestor/file/upload [post]
func doUpload(w http.ResponseWriter, r *http.Request) {
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

	objectTotalSize, err := internalUtils.ObjectTotalSizeGetAndValidate(r)
	if err != nil {
		w.Header().Add(ServiceErrorHeader, "can't get object total size value")
		w.Header().Add(ServiceErrorContentHeader, err.Error())
		w.WriteHeader(404)
		logs.MainLogger.Println(err)
		return
	}

	logs.MainLogger.Println("uploading object...")
	logs.MainLogger.Println("bucket:", objectBucket)
	logs.MainLogger.Println("key", objectKey)
	logs.MainLogger.Println("total size", objectTotalSize)

	bytesBuffer := make([]byte, 16*1024)

	totalSizeProcessed := 0

	partIdx := uint16(0)
	totalOffset := 0
	partDataBuffer := make([]byte, 0)
	doRead := true

	controller := http.NewResponseController(w)

	for doRead {
		controller.SetReadDeadline(time.Now().Add(5 * time.Second))

		n, err := r.Body.Read(bytesBuffer)
		if err != nil {
			if err == io.EOF {
				doRead = false
			} else {
				shards.EraseParts(objectBucket, objectKey)
				w.Header().Add(ServiceErrorHeader, "error while reading file")
				w.Header().Add(ServiceErrorContentHeader, err.Error())
				w.WriteHeader(500)
				logs.MainLogger.Println(err)
				return
			}
		}

		partDataBuffer = append(partDataBuffer, bytesBuffer[0:n]...)
		if len(partDataBuffer) >= ObjectPartSize {
			err = shards.UploadPart(internalTypes.ObjectPart{
				Bucket:            objectBucket,
				Key:               objectKey,
				Data:              &partDataBuffer,
				PartDataSize:      uint64(len(partDataBuffer)),
				TotalObjectOffset: uint64(totalOffset),
				TotalObjectSize:   objectTotalSize,
			}, partIdx)
			if err != nil {
				shards.EraseParts(objectBucket, objectKey)
				w.Header().Add(ServiceErrorHeader, "error while uploading file part")
				w.Header().Add(ServiceErrorContentHeader, err.Error())
				w.WriteHeader(500)
				return
			}

			totalOffset += len(partDataBuffer)
			partDataBuffer = make([]byte, 0)
			partIdx++
		}

		totalSizeProcessed += n
	}

	if len(partDataBuffer) > 0 {
		err = shards.UploadPart(internalTypes.ObjectPart{
			Bucket:            objectBucket,
			Key:               objectKey,
			Data:              &partDataBuffer,
			PartDataSize:      uint64(len(partDataBuffer)),
			TotalObjectOffset: uint64(totalOffset),
			TotalObjectSize:   objectTotalSize,
		}, partIdx)
		if err != nil {
			shards.EraseParts(objectBucket, objectKey)
			w.Header().Add(ServiceErrorHeader, "error while uploading file part")
			w.Header().Add(ServiceErrorContentHeader, err.Error())
			w.WriteHeader(500)
			return
		}
	}

	logs.MainLogger.Println("done", totalSizeProcessed)
}

// @Summary		Download file
//
// @Description	Download file by bucket name and key value
// @Produce		octet-stream
// @Param		X-Karma8-Object-Bucket	header	string true	"Bucket name for target file"
// @Param		X-Karma8-Object-Key		header	string true	"Key value for target file"
// @Success		200
// @Router		/ingestor/file/download [post]
func doDownload(w http.ResponseWriter, r *http.Request) {
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

	logs.MainLogger.Println("downloading object...")
	logs.MainLogger.Println("bucket:", objectBucket)
	logs.MainLogger.Println("key:", objectKey)

	parts, err := shards.DownloadPart(objectBucket, objectKey)
	if err != nil {
		w.Header().Add(ServiceErrorHeader, "error while downloading file parts")
		w.Header().Add(ServiceErrorContentHeader, err.Error())
		w.WriteHeader(500)
		logs.MainLogger.Println(err)
		return
	}

	totalSizeProcessed := 0

	w.Header().Set("Content-Type", "application/octet-stream")

	controller := http.NewResponseController(w)

	for part := range parts {
		if len(*part.Data) > 0 {
			controller.SetWriteDeadline(time.Now().Add(5 * time.Second))
			w.Write(*part.Data)
			totalSizeProcessed += len(*part.Data)
		}
	}

	logs.MainLogger.Println("done", totalSizeProcessed)
}

// @title API
// @version 1.0
// @description Ingestor (Karma8 Test Case)
// @termsOfService http://swagger.io/terms/

// @contact.name API Support
// @contact.url http://www.swagger.io/support
// @contact.email support@swagger.io

// @license.name Apache 2.0
// @license.url http://www.apache.org/licenses/LICENSE-2.0.html

// @host 127.0.0.1:7788
// @BasePath /
func runRestServer() {
	logs.MainLogger.Println("REST server...")

	targetServiceAddr := os.Getenv("INGESTOR_SERVICE_ADDR")
	targetServicePort := os.Getenv("INGESTOR_SERVICE_PORT")

	router := chi.NewRouter()

	if debugService == "yes" {
		swaggerUrl := fmt.Sprintf("http://%s:%s/swagger/doc.json", targetServiceAddr, targetServicePort)

		router.Use(cors.Handler(cors.Options{
			AllowedOrigins: []string{"https://*", "http://*", "http://0.0.0.0:7788"},
			AllowedMethods: []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
			AllowedHeaders: []string{
				"*",
				"Accept",
				"Authorization",
				"Content-Type",
				"X-CSRF-Token",
				"Access-Control-Allow-*",
				"Access-Control-Allow-Origin",
			},
			ExposedHeaders:   []string{"Link"},
			AllowCredentials: false,
			MaxAge:           300,
		}))

		router.Get("/swagger/*", httpSwagger.Handler(
			httpSwagger.URL(swaggerUrl),
			httpSwagger.BeforeScript(`const UrlMutatorPlugin = (system) => ({
			rootInjects: {
			  setScheme: (scheme) => {
				const jsonSpec = system.getState().toJSON().spec.json;
				const schemes = Array.isArray(scheme) ? scheme : [scheme];
				const newJsonSpec = Object.assign({}, jsonSpec, { schemes });
		  
				return system.specActions.updateJsonSpec(newJsonSpec);
			  },
			  setHost: (host) => {
				const jsonSpec = system.getState().toJSON().spec.json;
				const newJsonSpec = Object.assign({}, jsonSpec, { host });
		  
				return system.specActions.updateJsonSpec(newJsonSpec);
			  },
			  setBasePath: (basePath) => {
				const jsonSpec = system.getState().toJSON().spec.json;
				const newJsonSpec = Object.assign({}, jsonSpec, { basePath });
		  
				return system.specActions.updateJsonSpec(newJsonSpec);
			  }
			}
		  });`),
			httpSwagger.Plugins([]string{"UrlMutatorPlugin"}),
			httpSwagger.UIConfig(map[string]string{
				"onComplete": fmt.Sprintf(`() => {
    									window.ui.setScheme('%s');
    									window.ui.setHost('%s');
    									window.ui.setBasePath('%s');
  			}`, "http", targetServiceAddr+":"+targetServicePort, "/"),
			}),
		))
	}

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
