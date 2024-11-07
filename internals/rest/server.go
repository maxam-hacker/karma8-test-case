package rest

import (
	"net/http"
	"time"
)

type RestServer struct {
	HttpServer *http.Server
}

func NewHttpServer(address string, handler http.Handler) *RestServer {
	return &RestServer{
		HttpServer: &http.Server{
			Addr:         address,
			Handler:      handler,
			ReadTimeout:  10 * time.Second,
			WriteTimeout: 10 * time.Second,
		},
	}
}

func (rs *RestServer) Start() {
	rs.HttpServer.ListenAndServe()
}
