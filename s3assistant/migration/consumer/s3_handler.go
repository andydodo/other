package consumer

import (
	"time"

	"s3assistant/migration"
	"s3assistant/s3entry"
	"s3assistant/util"

	"go.uber.org/zap"
)

const maxMultiUploadPartCount = 10000

const OBJECT_CHANGED_FUNCTION = "OBJECT_CHANGED_FUNCTION"

type S3Handler struct {
	logger              *zap.Logger
	tempDir             string // directory to hold temp file in local
	sourceCli           *s3entry.S3Client
	targetCli           *s3entry.S3Client
	ministPartSizeBytes uint64
}

func NewS3Handler(tempDir string, ministPartSizeBytes uint64,
	sourceCli, targetCli *s3entry.S3Client, logger *zap.Logger) *S3Handler {
	return &S3Handler{
		tempDir:             tempDir,
		sourceCli:           sourceCli,
		targetCli:           targetCli,
		ministPartSizeBytes: ministPartSizeBytes,
		logger:              logger,
	}
}

func (s *S3Handler) runMigration(migrationItem *migration.MigrationItem) (err error) {
	// 0. Get object at the first time to fetch the first part and object size
	getObjectHandler := s3entry.FetchGetObjectHandle(s.tempDir,
		migrationItem.SrcBucketName, migrationItem.ObjectName,
		0, s.ministPartSizeBytes, s.sourceCli, s.logger, false)
	defer s3entry.RestoreGetObjectHandle(getObjectHandler)
	if err = getObjectHandler.GetObjectWithRetry(); err != nil {
		switch err.(type) {
		case *util.S3Error:
			// Mark success when object has changed
			if err.(*util.S3Error).Function == OBJECT_CHANGED_FUNCTION {
				return nil
			}
			return
		}
		return
	}
	// 1. Judge the object size with ministPartSizeBytes to decide how to migration
	if getObjectHandler.ObjectSize > s.ministPartSizeBytes {
		err = s.multiPartUploadMethod(migrationItem, getObjectHandler)
	} else {
		err = s.putObjectMethod(migrationItem, getObjectHandler)
	}
	if err != nil {
		switch err.(type) {
		case *util.S3Error:
			// Mark success when object has changed
			if err.(*util.S3Error).Function == OBJECT_CHANGED_FUNCTION {
				return nil
			}
			return
		}
	}
	return
}

// putObjectMethod function is called when the object size is less than ministPartSizeBytes
func (s *S3Handler) putObjectMethod(migrationItem *migration.MigrationItem,
	getObjectHandler *s3entry.GetObjectHandle) error {
	putHandler := s3entry.FetchPutHandle(getObjectHandler,
		migrationItem.DstBucketName,
		s.targetCli,
		s.logger)
	defer s3entry.RestorePutHandle(putHandler)
	return putHandler.PutObjectWithRetry()
}

// MultiPartUploadiMethod is called when objectSize is larger than ministPartSizeBytes
func (s *S3Handler) multiPartUploadMethod(
	migrationItem *migration.MigrationItem, getObjectHandler *s3entry.GetObjectHandle) error {
	multiPartUploadHandler := s3entry.FetchMultiPartUploadHandle(
		getObjectHandler, migrationItem.DstBucketName, s.targetCli, s.logger)
	defer s3entry.RestoreMultiPartUploadHandle(multiPartUploadHandler)
	return multiPartUploadHandler.Run()
}

func (s *S3Handler) runMigrationWithRetry(migrationItem *migration.MigrationItem) error {
	runMigrationRetry := s3entry.S3_RETRY_TIMES
	var err error
	for runMigrationRetry > 0 {
		err = s.runMigration(migrationItem)
		if err != nil {
			s.logger.Warn("RunMigration error occurred, retry",
				zap.String("error", err.Error()),
				zap.Object("MigarationInfo", migrationItem))
			runMigrationRetry -= 1
			time.Sleep(time.Second * time.Duration(
				(s3entry.S3_RETRY_TIMES-runMigrationRetry)*s3entry.S3_RETRY_SLEEP_SECONDS))
			continue
		}
		s.logger.Info("RunMigration successfully",
			zap.Object("MigarationInfo", migrationItem))
		return nil
	}
	return err
}
