package rest

import "net/http"

type RestServer struct {
	HttpServer *http.Server
}

func NewHttpServer(address string, handler http.Handler) *RestServer {
	return &RestServer{
		HttpServer: &http.Server{
			Addr:    address,
			Handler: handler,
		},
	}
}

func (rs *RestServer) Start() {
	rs.HttpServer.ListenAndServe()
}
