package main

import (
	"fmt"
	"net/http"
	"os"
	"s3assistant/common"
	"s3assistant/s3entry"
	"testing"
)

func TestPutObject(t *testing.T) {
	// t.Fatal("not implemented")
	var err error
	s3conf := &s3entry.S3Conf{
		Endpoint:  "127.0.0.1:80",
		AccessKey: "xxx",
		SecretKey: "xxx",
	}
	logger, err := common.LoggerWrapper.CreateLogger("./", "test-log", 0)
	if err != nil {
		fmt.Printf("create logger error: %s\n", err)
		return
	}
	client := s3entry.NewS3Client(logger, s3conf, http.DefaultClient)
	file, err := os.OpenFile("./testfile", os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		fmt.Printf("create temp file error: %s\n", err)
		return
	}
	d1 := []byte("hello\ngo\n")
	_, err = file.Write(d1)
	if err != nil {
		fmt.Printf("write temp file error: %s\n", err)
		return
	}
	bucketName := "test-bucket"
	objectName := "test-object"
	err = client.PutObject(file, bucketName, objectName)
	if err != nil {
		fmt.Printf("put-object error: %s\n", err)
		return
	}
}
