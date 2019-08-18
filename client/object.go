package s3client

import (
	"bytes"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"io/ioutil"
	"log"
)

func (sc *S3Client) PutObject(key string, f []byte) (string, error) {
	creds := credentials.NewStaticCredentials(sc.S3AccessKey, sc.S3SecretKey, "")
	config := &aws.Config{
		Region:           aws.String(sc.S3Region),
		Endpoint:         aws.String(sc.S3EndPoint),
		Credentials:      creds,
		S3ForcePathStyle: aws.Bool(true),
	}
	sess := session.Must(session.NewSession(config))
	service := s3.New(sess)
	_, err := service.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(sc.S3Bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(f),
	})
	if err != nil {
		log.Printf("service.PutObject. error(%v)", err)
		return "", err
	}

	//可以自己拼出来
	path := sc.S3EndPoint + "/" + sc.S3Bucket + "/" + key
	return path, nil
}

func (sc *S3Client) GetObject(key string) ([]byte, error) {
	creds := credentials.NewStaticCredentials(sc.S3AccessKey, sc.S3SecretKey, "")
	config := &aws.Config{
		Region:           aws.String(sc.S3Region),
		Endpoint:         aws.String(sc.S3EndPoint),
		Credentials:      creds,
		S3ForcePathStyle: aws.Bool(true),
	}
	sess := session.Must(session.NewSession(config))
	service := s3.New(sess)

	input := &s3.GetObjectInput{
		Bucket: aws.String(sc.S3Bucket),
		Key:    aws.String(key),
		//Range:  aws.String("bytes=0-9"), // to retrieve a specific byte range
	}

	result, err := service.GetObject(input)
	defer result.Body.Close()
	if err != nil {
		log.Printf("service.GetObject(key:%v). error(%v)", key, err.Error())
		return nil, err
	}

	data, err := ioutil.ReadAll(result.Body)
	if err != nil {
		log.Printf("ioutil.ReadAll(). error(%v)", err.Error())
		return nil, err
	}
	return data, nil
}

func (sc *S3Client) DeleteObject(key string) error {
	creds := credentials.NewStaticCredentials(sc.S3AccessKey, sc.S3SecretKey, "")
	config := &aws.Config{
		Region:      aws.String(sc.S3Region),
		Endpoint:    aws.String(sc.S3EndPoint),
		Credentials: creds,
	}
	sess := session.Must(session.NewSession(config))
	service := s3.New(sess)

	input := &s3.DeleteObjectInput{
		Bucket: aws.String(sc.S3Bucket),
		Key:    aws.String(key),
	}

	_, err := service.DeleteObject(input)
	if err != nil {
		log.Printf("service.DeleteObject(key:%v). error(%v)", key, err.Error())
		return err
	}

	return nil
}
