package utils

import (
	"errors"
	"net/http"
	"strconv"
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

func ObjectTotalSizeGetAndValidate(r *http.Request) (uint64, error) {
	objectTotalSize := r.Header.Get("X-Karma8-Object-Total-Size")
	if objectTotalSize == "" {
		return 0, ErrBadObjectTotalSizeValue
	}

	size, err := strconv.ParseUint(objectTotalSize, 10, 0)
	if err != nil {
		return 0, ErrBadObjectTotalSizeValue
	}

	return size, nil
}

func ObjectTotalOffsetGetAndValidate(r *http.Request) (uint64, error) {
	objectTotalOffset := r.Header.Get("X-Karma8-Object-Total-Offset")
	if objectTotalOffset == "" {
		return 0, ErrBadObjectTotalOffsetValue
	}

	offset, err := strconv.ParseUint(objectTotalOffset, 10, 0)
	if err != nil {
		return 0, ErrBadObjectTotalOffsetValue
	}

	return offset, nil
}

func ObjectPartDataSizeGetAndValidate(r *http.Request) (uint64, error) {
	partDataSize := r.Header.Get("X-Karma8-Object-Part-Data-Size")
	if partDataSize == "" {
		return 0, ErrBadObjectPardDataSizeValue
	}

	size, err := strconv.ParseUint(partDataSize, 10, 0)
	if err != nil {
		return 0, ErrBadObjectPardDataSizeValue
	}

	return size, nil
}
