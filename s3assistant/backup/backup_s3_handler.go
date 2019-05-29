package backup

import (
	"context"
	"strconv"
	"sync"
	"time"

	"s3assistant/common"
	"s3assistant/s3entry"
	"s3assistant/util"

	"go.uber.org/zap"
)

const (
	GET_OBJECT_FUNCTION     = "GetObject"
	OBJECT_CHANGED_FUNCTION = "OBJECT_CHANGED_FUNCTION"
)

type S3Handler struct {
	sourceCli, targetCli      *s3entry.S3Client
	destinationBucket         string // the destination bucket
	ministPartSizeBytes       uint64
	failedChannel             chan struct{} // failedChannel used to mark wait status
	completedChannel          chan struct{} // completedChannel used to mark success status
	keepAliveWaitGroup        sync.WaitGroup
	keepAliveSleepTimeSeconds int64
	logger                    *zap.Logger
	tempDir                   string
}

func NewS3Handler(logger *zap.Logger, destinationBucket, tempDir string, ministPartSizeBytes uint64,
	keepAliveSleepTimeSeconds int64, sourceCli, targetCli *s3entry.S3Client) *S3Handler {
	return &S3Handler{
		sourceCli:                 sourceCli,
		targetCli:                 targetCli,
		destinationBucket:         destinationBucket,
		ministPartSizeBytes:       ministPartSizeBytes,
		completedChannel:          make(chan struct{}),
		failedChannel:             make(chan struct{}),
		keepAliveSleepTimeSeconds: keepAliveSleepTimeSeconds,
		logger:  logger,
		tempDir: tempDir,
	}
}

func (s *S3Handler) RunBackup(backupSourceInfo *BackupSourceInfo, redisHandler *BackupRedisHandler) error {
	// 0. Initial
	s.keepAliveWaitGroup.Add(1)
	// 1. Start goroutine to keep alive
	cancel := s.StartKeepAliveGoroutine(backupSourceInfo, redisHandler)
	defer s.keepAliveWaitGroup.Wait()
	defer cancel()

	// 2. Run
	var err error
	if s.destinationBucket == common.EMPTY_STRING {
		backupSourceInfo.dstBucketName = backupSourceInfo.srcBucketName
	} else {
		backupSourceInfo.dstBucketName = s.destinationBucket
	}
	// 2.1 Add operation
	if backupSourceInfo.operation == PUT_OPERATION {
		if backupSourceInfo.objectSize > s.ministPartSizeBytes {
			err = s.MultiPartUploadMethod(backupSourceInfo)
		} else {
			err = s.PutObjectMethod(backupSourceInfo)
		}
	}
	// 2.2 Delete operation
	if backupSourceInfo.operation == DELETE_OPERATION {
		err = s.DeleteObjectMethod(backupSourceInfo)
	}
	if err != nil {
		switch err.(type) {
		case *util.S3Error:
			// Mark success when object has changed
			if err.(*util.S3Error).Function == OBJECT_CHANGED_FUNCTION {
				s.completedChannel <- struct{}{}
				return nil
			}
		}
		// do not redeliver delete operation
		if backupSourceInfo.operation != DELETE_OPERATION {
			s.failedChannel <- struct{}{}
			return err
		}
	}
	s.completedChannel <- struct{}{}
	return nil
}

func (s *S3Handler) StartKeepAliveGoroutine(backupSourceInfo *BackupSourceInfo,
	redisHandler *BackupRedisHandler) context.CancelFunc {
	ctx, cancel := context.WithCancel(context.Background())
	go func(ctx context.Context) {
		defer s.keepAliveWaitGroup.Done()
		defer s.logger.Sync()
		for {
			select {
			case <-ctx.Done():
				return
			case <-s.failedChannel:
				// Mark item status to WAITING_TIMESTAMP_STRING first to indicate it will be requeue in the future,
				// avoiding checker redeliver it twice which will lead to memory leak.
				ok := redisHandler.hsetBackupItemTimeToKeepAliveWithRetry(backupSourceInfo, WAITING_TIMESTAMP_STRING)
				if !ok {
					s.logger.Error("hsetBackupItemTimeToKeepAliveWithRetry failed, mark waiting status failed",
						zap.Object("backupSourceInfo", backupSourceInfo))
				}
				return
			case <-s.completedChannel:
				ok := redisHandler.hsetBackupItemTimeToKeepAliveWithRetry(backupSourceInfo, COMPLETED_TIMESTAMP_STRING)
				if !ok {
					s.logger.Error("hsetBackupItemTimeToKeepAliveWithRetry failed, mark completed status failed",
						zap.Object("backupSourceInfo", backupSourceInfo))
				}
				return
			default:
				ok := redisHandler.hsetBackupItemTimeToKeepAliveWithRetry(backupSourceInfo, strconv.FormatInt(time.Now().Unix(), 10))
				if !ok {
					s.logger.Error("hsetBackupItemTimeToKeepAliveWithRetry failed, keep alive failed",
						zap.Object("backupSourceInfo", backupSourceInfo))
					return
				}
				// sleep for a while for next update operation
				time.Sleep(time.Second * time.Duration(s.keepAliveSleepTimeSeconds))
			}
		}
	}(ctx)
	return cancel
}

// DeleteObjectMethod function is called to delete object
func (s *S3Handler) DeleteObjectMethod(backupSourceInfo *BackupSourceInfo) error {
	deleteHandler := s3entry.FetchDeleteHandle(backupSourceInfo.dstBucketName,
		backupSourceInfo.objectName, s.targetCli, s.logger)
	defer s3entry.RestoreDeleteHandle(deleteHandler)
	return deleteHandler.DeleteObjectWithRetry()
}

// PutObjectMethod function is called when objectSize is less than ministPartSizeBytes
func (s *S3Handler) PutObjectMethod(backupSourceInfo *BackupSourceInfo) (err error) {
	getObjectHandler := s3entry.FetchGetObjectHandle(s.tempDir,
		backupSourceInfo.srcBucketName,
		backupSourceInfo.objectName,
		backupSourceInfo.objectSize,
		s.ministPartSizeBytes,
		s.sourceCli, s.logger, true)
	defer s3entry.RestoreGetObjectHandle(getObjectHandler)
	putHandler := s3entry.FetchPutHandle(getObjectHandler,
		backupSourceInfo.dstBucketName, s.targetCli, s.logger)
	defer s3entry.RestorePutHandle(putHandler)
	if err = putHandler.GetObjectHandler.GetObjectWithRetry(); err != nil {
		return
	}
	if err = putHandler.PutObjectWithRetry(); err != nil {
		return
	}
	return nil
}

// MultiPartUploadiMethod is called when objectSize is larger than ministPartSizeBytes
func (s *S3Handler) MultiPartUploadMethod(backupSourceInfo *BackupSourceInfo) error {
	getObjectHandler := s3entry.FetchGetObjectHandle(s.tempDir,
		backupSourceInfo.srcBucketName,
		backupSourceInfo.objectName,
		backupSourceInfo.objectSize,
		s.ministPartSizeBytes,
		s.sourceCli, s.logger, true)
	defer s3entry.RestoreGetObjectHandle(getObjectHandler)
	multiPartUploadHandler := s3entry.FetchMultiPartUploadHandle(
		getObjectHandler,
		backupSourceInfo.dstBucketName,
		s.targetCli, s.logger)
	defer s3entry.RestoreMultiPartUploadHandle(multiPartUploadHandler)
	return multiPartUploadHandler.Run()
}
