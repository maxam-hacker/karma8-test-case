package rest

import (
	"fmt"
	"io"
	"net/http"
)

func SendServiceRequest(address string, body io.Reader) (*http.Response, error) {
	response, err := http.Post(address, "application/json", body)
	if err != nil {
		return nil, err
	}

	serviceErrorMessage := response.Header.Get("X-Karma8-Internal-Service-Error")
	serviceErrorContent := response.Header.Get("X-Karma8-Internal-Service-Error-Content")

	if serviceErrorMessage != "" {
		return nil, fmt.Errorf("service error: %s; details: %s", serviceErrorMessage, serviceErrorContent)
	}

	return response, nil
}
