// Package api Code generated by swaggo/swag. DO NOT EDIT
package api

import "github.com/swaggo/swag"

const docTemplate = `{
    "schemes": {{ marshal .Schemes }},
    "swagger": "2.0",
    "info": {
        "description": "{{escape .Description}}",
        "title": "{{.Title}}",
        "termsOfService": "http://swagger.io/terms/",
        "contact": {
            "name": "API Support",
            "url": "http://www.swagger.io/support",
            "email": "support@swagger.io"
        },
        "license": {
            "name": "Apache 2.0",
            "url": "http://www.apache.org/licenses/LICENSE-2.0.html"
        },
        "version": "{{.Version}}"
    },
    "host": "{{.Host}}",
    "basePath": "{{.BasePath}}",
    "paths": {
        "/ingestor/file/download": {
            "post": {
                "description": "Download file by bucket name and key value",
                "produces": [
                    "application/octet-stream"
                ],
                "summary": "Download file",
                "parameters": [
                    {
                        "type": "string",
                        "description": "Bucket name for target file",
                        "name": "X-Karma8-Object-Bucket",
                        "in": "header",
                        "required": true
                    },
                    {
                        "type": "string",
                        "description": "Key value for target file",
                        "name": "X-Karma8-Object-Key",
                        "in": "header",
                        "required": true
                    }
                ],
                "responses": {
                    "200": {
                        "description": "OK"
                    }
                }
            }
        },
        "/ingestor/file/upload": {
            "post": {
                "description": "Upload file with bucket name and key value",
                "consumes": [
                    "application/octet-stream"
                ],
                "summary": "Upload file",
                "parameters": [
                    {
                        "type": "string",
                        "description": "Bucket name for target file",
                        "name": "X-Karma8-Object-Bucket",
                        "in": "header",
                        "required": true
                    },
                    {
                        "type": "string",
                        "description": "Key value for target file",
                        "name": "X-Karma8-Object-Key",
                        "in": "header",
                        "required": true
                    },
                    {
                        "type": "string",
                        "description": "Total size of target file",
                        "name": "X-Karma8-Object-Total-Size",
                        "in": "header",
                        "required": true
                    },
                    {
                        "type": "file",
                        "description": "Target file",
                        "name": "file",
                        "in": "formData",
                        "required": true
                    }
                ],
                "responses": {
                    "200": {
                        "description": "OK"
                    }
                }
            }
        }
    }
}`

// SwaggerInfo holds exported Swagger Info so clients can modify it
var SwaggerInfo = &swag.Spec{
	Version:          "1.0",
	Host:             "127.0.0.1:7788",
	BasePath:         "/",
	Schemes:          []string{},
	Title:            "API",
	Description:      "Ingestor (Karma8 Test Case)",
	InfoInstanceName: "swagger",
	SwaggerTemplate:  docTemplate,
	//LeftDelim:        "{{",
	//RightDelim:       "}}",
}

func init() {
	swag.Register(SwaggerInfo.InstanceName(), SwaggerInfo)
}