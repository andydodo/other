package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

var (
	filename string
)

var usage = `Usage: silence [options...] <path>

  Options:
	      -f "file name path"
`

func init() {
	flag.StringVar(&filename, "f", "", "filenama path")
}

func main() {
	flag.Usage = func() {
		fmt.Fprint(os.Stderr, usage)
	}
	flag.Parse()
	svc := newClient("http://10.29.3.17:80", "e8nMRTDu3WE1", "aNU34yPAGNtXE1yv0kZrk4")

	ticker := time.NewTicker(3 * time.Hour)
	for {
		select {
		case <-ticker.C:
			result, err := uploadFile(svc)
			if err != nil {
				log.Println("upload file failed")
				return
			}
			fmt.Println(result)
		}
	}
}

func newClient(endpoint string, key string, secret string) *session.Session {
	sess, err := session.NewSession(&aws.Config{
		Region:           aws.String("default"),
		Credentials:      credentials.NewStaticCredentials(key, secret, ""),
		S3ForcePathStyle: aws.Bool(true),
		Endpoint:         aws.String(endpoint),
	})
	if err != nil {
		fmt.Printf("Cannot create new s3 seesion, %v", err)
	}
	client := session.Must(sess, nil)
	return client
}

func uploadFile(svc *session.Session) (string, error) {
	uploader := s3manager.NewUploader(svc)

	f, err := os.Open("/data02/obj40G")
	if err != nil {
		log.Printf("failed to open file %q, %v", filename, err)
		return "", err
	}

	// Upload the file to S3.
	result, err := uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String("s3-monitor"),
		Key:    &filename,
		Body:   f,
	})
	if err != nil {
		log.Printf("failed to upload file, %v", err)
		return "", err
	}
	path := result.Location
	return path, nil
}
