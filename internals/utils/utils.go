package utils

import (
	"errors"
	"net/http"
)

var (
	ErrBadObjectBucketName        = errors.New("bad object bucket name value")
	ErrBadObjectKeyValue          = errors.New("bad object key value")
	ErrBadObjectTotalSizeValue    = errors.New("bad object total size value")
	ErrBadObjectTotalOffsetValue  = errors.New("bad object total offset value")
	ErrBadObjectPardDataSizeValue = errors.New("bad object part data size value")
)

func ObjectBucketGetAndValidate(r *http.Request) (string, error) {
	objectBucket := r.Header.Get("X-Karma8-Object-Bucket")
	if objectBucket == "" {
		return "", ErrBadObjectBucketName
	}
	return objectBucket, nil
}

func ObjectKeyGetAndValidate(r *http.Request) (string, error) {
	objectKey := r.Header.Get("X-Karma8-Object-Key")
	if objectKey == "" {
		return "", ErrBadObjectKeyValue
	}
	return objectKey, nil
}

func ObjectTotalSizeGetAndValidate(r *http.Request) (string, error) {
	objectTotalSize := r.Header.Get("X-Karma8-Object-Total-Size")
	if objectTotalSize == "" {
		return "", ErrBadObjectTotalSizeValue
	}
	return objectTotalSize, nil
}

func ObjectTotalObjectOffsetGetAndValidate(r *http.Request) (string, error) {
	objectTotalOffset := r.Header.Get("X-Karma8-Object-Total-Offset")
	if objectTotalOffset == "" {
		return "", ErrBadObjectTotalOffsetValue
	}
	return objectTotalOffset, nil
}

func ObjectPartDataSizeGetAndValidate(r *http.Request) (string, error) {
	partDataSize := r.Header.Get("X-Karma8-Object-Part-Data-Size")
	if partDataSize == "" {
		return "", ErrBadObjectPardDataSizeValue
	}
	return partDataSize, nil
}
