package s3entry

import (
	"fmt"
	"net/http"
	"os"
	"s3assistant/common"
	"sort"
	"strconv"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"go.uber.org/zap"
)

const (
	REGION = "cn-north-1"
)

type S3Conf struct {
	Endpoint  string `yaml:"endpoint"`
	AccessKey string `yaml:"access_key"`
	SecretKey string `yaml:"secret_key"`
}

func NewS3Conf(endpoint, accessKey, secertKey string) *S3Conf {
	return &S3Conf{
		Endpoint:  endpoint,
		AccessKey: accessKey,
		SecretKey: secertKey,
	}
}

type S3Client struct {
	client *s3.S3
	logger *zap.Logger
}

func NewS3Client(logger *zap.Logger, s3Conf *S3Conf, httpClient *http.Client) *S3Client {
	sess := session.Must(session.NewSession(&aws.Config{
		HTTPClient: httpClient,
		Credentials: credentials.NewStaticCredentialsFromCreds(credentials.Value{
			AccessKeyID:     s3Conf.AccessKey,
			SecretAccessKey: s3Conf.SecretKey,
		}),
		Region:           aws.String(REGION),
		Endpoint:         aws.String(s3Conf.Endpoint),
		S3ForcePathStyle: aws.Bool(true),
	}))
	return &S3Client{
		logger: logger,
		client: s3.New(sess),
	}
}

func (s *S3Client) GetObjectRangeWithoutVersionId(
	bucketName, objectName string, startIndex, size uint64) (*s3.GetObjectOutput, error) {
	ranges := "bytes=" + strconv.FormatUint(startIndex, 10) + "-" + strconv.FormatUint(startIndex+size-1, 10)
	resp, err := s.client.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectName),
		Range:  aws.String(ranges),
	})
	return resp, err
}

func (s *S3Client) GetObjectRangeWithVersionId(
	bucketName, objectName string, lastModified string, startIndex, size uint64) (*s3.GetObjectOutput, error) {
	ranges := "bytes=" + strconv.FormatUint(startIndex, 10) + "-" + strconv.FormatUint(startIndex+size-1, 10)
	resp, err := s.client.GetObject(&s3.GetObjectInput{
		Bucket:    aws.String(bucketName),
		Key:       aws.String(objectName),
		Range:     aws.String(ranges),
		VersionId: aws.String(lastModified),
	})
	return resp, err
}

func (s *S3Client) CreateBucket(bucketName string) error {
	_, err := s.client.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			s.logger.Error("CreateBucket awsError occurred",
				zap.String("Bucket", bucketName),
				zap.String("errorCode", awsErr.Code()),
				zap.String("errorMessage", awsErr.Message()))
		} else {
			s.logger.Error("CreateBucket unexpected error occurred",
				zap.String("error", err.Error()))
		}
		return err
	}
	return nil
}

func (s *S3Client) InitMultiUpload(bucketName, objectName string) (*s3.CreateMultipartUploadOutput, error) {
	result, err := s.client.CreateMultipartUpload(&s3.CreateMultipartUploadInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectName),
	})
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			s.logger.Error("InitMultiUpload awsError occurred",
				zap.String("Bucket", bucketName),
				zap.String("Key", objectName),
				zap.String("errorCode", awsErr.Code()),
				zap.String("errorMessage", awsErr.Message()))
		} else {
			s.logger.Error("InitMultiUpload unexpected error occurred", zap.String("error", err.Error()))
		}
		return nil, err
	}
	return result, nil
}

func (s *S3Client) UploadPart(bucketName string,
	objectName string, partNum int64, uploadId string, file *os.File) (*s3.UploadPartOutput, error) {
	result, err := s.client.UploadPart(&s3.UploadPartInput{
		Body:       file,
		Bucket:     aws.String(bucketName),
		Key:        aws.String(objectName),
		PartNumber: aws.Int64(partNum),
		UploadId:   aws.String(uploadId),
	})
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			s.logger.Error("UploadPart awsError occurred",
				zap.String("Bucket", bucketName),
				zap.String("Key", objectName),
				zap.String("UploadId", uploadId),
				zap.String("errorCode", awsErr.Code()),
				zap.String("errorMessage", awsErr.Message()))
		} else {
			s.logger.Error(err.Error())
		}
		return nil, err
	}
	return result, nil
}

func (s *S3Client) CompleteMultiUpload(bucketName string,
	objectName string, partsInfo map[int64]string, uploadId string) error {
	var partsSlice []*s3.CompletedPart
	var keys []int64
	for key := range partsInfo {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	for _, key := range keys {
		partsSlice = append(partsSlice, &s3.CompletedPart{
			ETag:       aws.String(partsInfo[key]),
			PartNumber: aws.Int64(key),
		})
	}
	fmt.Println(partsInfo)
	_, err := s.client.CompleteMultipartUpload(&s3.CompleteMultipartUploadInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectName),
		MultipartUpload: &s3.CompletedMultipartUpload{
			Parts: partsSlice,
		},
		UploadId: aws.String(uploadId),
	})
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			s.logger.Error("CompleteMultiUpload awsError occurred",
				zap.String("Bucket", bucketName),
				zap.String("Key", objectName),
				zap.String("UploadId", uploadId),
				zap.String("errorCode", awsErr.Code()),
				zap.String("errorMessage", awsErr.Message()))
			switch awsErr.Code() {
			// 404 is ok
			case s3.ErrCodeNoSuchUpload:
				return nil
			}
		} else {
			s.logger.Error("CompleteMultiUpload unexpected error occurred",
				zap.String("error", err.Error()))
		}
		return err
	}
	return nil
}

func (s *S3Client) AbortMultiUpload(bucketName, objectName, uploadId string) error {
	_, err := s.client.AbortMultipartUpload(&s3.AbortMultipartUploadInput{
		Bucket:   aws.String(bucketName),
		Key:      aws.String(objectName),
		UploadId: aws.String(uploadId),
	})
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			s.logger.Error("AbortMultiUpload awsError occurred",
				zap.String("Bucket", bucketName),
				zap.String("Key", objectName),
				zap.String("UploadId", uploadId),
				zap.String("errorCode", awsErr.Code()),
				zap.String("errorMessage", awsErr.Message()))
			switch awsErr.Code() {
			// 404 is ok
			case s3.ErrCodeNoSuchUpload:
				return nil
			}
		} else {
			s.logger.Error("AbortMultiUpload unexpected error occurred",
				zap.String("error", err.Error()))
		}
		return err
	}
	return nil
}

func (s *S3Client) PutObject(file *os.File,
	bucketName, objectName string) error {
	metadata := make(map[string]*string)
	metadata["test"] = aws.String("test-value")
	_, err := s.client.PutObject(&s3.PutObjectInput{
		Metadata: metadata,
		Body:     file,
		Bucket:   aws.String(bucketName),
		Key:      aws.String(objectName),
	})
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			s.logger.Error("PutObject awsError occurred",
				zap.String("Bucket", bucketName),
				zap.String("Key", objectName),
				zap.String("errorCode", awsErr.Code()),
				zap.String("errorMessage", awsErr.Message()))
		} else {
			s.logger.Error("PutObject unexpected error occurred",
				zap.String("error", err.Error()))
		}
		return err
	}
	return nil
}

func (s *S3Client) DeleteObject(bucketName, objectName string) error {
	_, err := s.client.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectName),
	})

	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			s.logger.Error("DeleteObject awsError occurred",
				zap.String("Bucket", bucketName),
				zap.String("Key", objectName),
				zap.String("errorCode", awsErr.Code()),
				zap.String("errorMessage", awsErr.Message()))
			switch awsErr.Code() {
			// 404 is ok
			case s3.ErrCodeNoSuchKey:
				return nil
			default:
			}
		} else {
			s.logger.Error("DeleteObject unexpected error occurred",
				zap.String("error", err.Error()))
		}
		return err
	}
	return nil
}

func (s *S3Client) ListObjects(bucketName string,
	maxKeys int64) (*s3.ListObjectsOutput, error) {
	input := &s3.ListObjectsInput{
		Bucket:  aws.String(bucketName),
		MaxKeys: aws.Int64(maxKeys),
	}

	result, err := s.client.ListObjects(input)
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			s.logger.Error("ListObjects awsError occurred",
				zap.String("Bucket", bucketName),
				zap.Int64("MaxKeys", maxKeys),
				zap.String("errorCode", awsErr.Code()),
				zap.String("errorMessage", awsErr.Message()))
		} else {
			s.logger.Error("ListObjects unexpected errror occurred",
				zap.String("Bucket", bucketName),
				zap.Int64("MaxKeys", maxKeys),
				zap.String("errorMessage", err.Error()))
		}
		return nil, err
	}
	return result, nil
}

func (s *S3Client) ListObjectsV2(bucketName string,
	continuationToken *string, maxKeys int64) (*s3.ListObjectsV2Output, error) {
	input := &s3.ListObjectsV2Input{
		Bucket:            aws.String(bucketName),
		ContinuationToken: aws.String(*continuationToken),
		MaxKeys:           aws.Int64(maxKeys),
		StartAfter:        aws.String(*continuationToken),
	}

	result, err := s.client.ListObjectsV2(input)
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			s.logger.Error("ListObjectsV2 awsError occurred",
				zap.String("Bucket", bucketName),
				zap.String("ContinuationToken", *continuationToken),
				zap.Int64("MaxKeys", maxKeys),
				zap.String("errorCode", awsErr.Code()),
				zap.String("errorMessage", awsErr.Message()))
		} else {
			s.logger.Error("ListObjectsV2 unexpected errror occurred",
				zap.String("Bucket", bucketName),
				zap.String("ContinuationToken", *continuationToken),
				zap.Int64("MaxKeys", maxKeys),
				zap.String("errorMessage", err.Error()))
		}
		return nil, err
	}
	if result.NextContinuationToken == nil {
		*continuationToken = common.EMPTY_STRING
		return result, nil
	}
	// Fetch the continuationToken for next list operation
	*continuationToken = *result.NextContinuationToken
	return result, nil
}
