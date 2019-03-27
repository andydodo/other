package s3core

import (
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

var (
	emptyerr error = errors.New("this bucket is empty")
)

func NewClient(endpoint string, key string, secret string) *s3.S3 {
	sess, err := session.NewSession(&aws.Config{
		Region:           aws.String("default"),
		Credentials:      credentials.NewStaticCredentials(key, secret, ""),
		S3ForcePathStyle: aws.Bool(true),
		Endpoint:         aws.String(endpoint),
	})
	if err != nil {
		fmt.Printf("Cannot create new s3 seesion, %v", err)
	}
	client := s3.New(sess)
	return client
}

func ListObjs(svc *s3.S3, bucket string, t *string, ch chan string) error {

	input := &s3.ListObjectsV2Input{
		Bucket:            aws.String(bucket),
		MaxKeys:           aws.Int64(1000),
		ContinuationToken: aws.String(*t),
	}

	result, err := svc.ListObjectsV2(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case s3.ErrCodeNoSuchBucket:
				fmt.Println(s3.ErrCodeNoSuchBucket, aerr.Error())
			default:
				fmt.Println(aerr.Error())
			}
		} else {
			fmt.Println(err.Error())
		}
		return err
	}

	if result.NextContinuationToken == nil {
		return errors.New("bucket is empty")
	}

	for _, objname := range result.Contents {
		ch <- *objname.Key
	}

	*t = *result.NextContinuationToken
	return nil
}

func DeleteObj(svc *s3.S3, bucket, obj string) error {
	input := &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(obj),
	}

	_, err := svc.DeleteObject(input)
	if err != nil {
		fmt.Printf("%s: delete error\n", obj)
		return err
	}
	fmt.Printf("%s: delete success\n", obj)
	return nil
}
