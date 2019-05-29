package s3entry

import (
	"fmt"
	"s3assistant/util"
	"sync"
	"time"

	"go.uber.org/zap"
)

type MultiPartUploadHandle struct {
	GetObjectHandler *GetObjectHandle

	DstBucketName  string
	UploadId       string
	PartsInfo      map[int64]string
	NextPartNumber int64
	targetCli      *S3Client
	logger         *zap.Logger
}

func (m *MultiPartUploadHandle) Reset() {
	m.GetObjectHandler = nil
	m.DstBucketName = EMPTY_STRING
	m.UploadId = EMPTY_STRING
	m.PartsInfo = nil
	m.NextPartNumber = 1
	m.targetCli = nil
	m.logger = nil
}

func (m *MultiPartUploadHandle) Run() (err error) {
	// check
	if m.GetObjectHandler == nil {
		return &util.ParseError{
			Message: fmt.Sprintf("GetObjectHandler is nil")}
	}
	return m.multiPartUploadCore()
}

func (m *MultiPartUploadHandle) multiPartUploadCore() (err error) {
	defer func() {
		// Abort multi part upload
		if err != nil {
			_ = m.abortMultiUploadWithRetry()
		}
	}()
	// initialize
	if err = m.initMultiUploadWithRetry(); err != nil {
		return
	}
	for {
		if !m.GetObjectHandler.IsComplete() {
			// download
			if err = m.GetObjectHandler.GetObjectWithRetry(); err != nil {
				return
			}
			// upload
			if err = m.uploadPartWithRetry(); err != nil {
				return
			}
			// invoke Commit function to mark current part has been uploaded successfully,
			// downloader should update the state for next download operation.
			m.GetObjectHandler.Commit()
			continue
		}
		break
	}
	// Completed
	if err = m.completedMultipartWithRetry(); err != nil {
		return
	}
	return nil
}

func (m *MultiPartUploadHandle) initMultiUploadWithRetry() error {
	initMultiRetry := S3_RETRY_TIMES
	for initMultiRetry > 0 {
		result, err := m.targetCli.InitMultiUpload(m.DstBucketName,
			m.GetObjectHandler.ObjectName)
		if err != nil || result == nil {
			m.logger.Error("InitMultiUpload failed, retry")
			initMultiRetry -= 1
			time.Sleep(time.Second * time.Duration((S3_RETRY_TIMES-initMultiRetry)*S3_RETRY_SLEEP_SECONDS))
			continue
		}
		m.UploadId = *(result.UploadId)
		m.logger.Info("InitMultiUpload successfully",
			zap.String("UploadId", m.UploadId))
		return nil
	}
	return &util.S3Error{
		Bucket:   m.DstBucketName,
		Key:      m.GetObjectHandler.ObjectName,
		Function: CREATE_MULTIPART_UPLOAD_FUNCTION,
	}
}

func (m *MultiPartUploadHandle) uploadPartWithRetry() error {
	uploadPartRetry := S3_RETRY_TIMES
	for uploadPartRetry > 0 {
		m.GetObjectHandler.FdHandler.Fd.Seek(0, 0)
		uploadPartResponse, err := m.targetCli.UploadPart(m.DstBucketName,
			m.GetObjectHandler.ObjectName,
			m.NextPartNumber,
			m.UploadId,
			m.GetObjectHandler.FdHandler.Fd)
		if err != nil || uploadPartResponse == nil {
			m.logger.Error("Uploadpart error occurred",
				zap.String("UploadId", m.UploadId),
				zap.Int64("PartNumber", m.NextPartNumber))
			uploadPartRetry -= 1
			time.Sleep(time.Second * time.Duration((S3_RETRY_TIMES-uploadPartRetry)*S3_RETRY_SLEEP_SECONDS))
			continue
		}
		m.logger.Info("UploadPart successfully",
			zap.String("UploadId", m.UploadId),
			zap.Int64("PartNumber", m.NextPartNumber))
		m.PartsInfo[m.NextPartNumber] = *(uploadPartResponse.ETag)
		m.NextPartNumber += 1
		return nil
	}
	return &util.S3Error{
		Bucket:   m.DstBucketName,
		Key:      m.GetObjectHandler.ObjectName,
		Function: UPLOAD_PART_FUNCTION,
	}
}

func (m *MultiPartUploadHandle) completedMultipartWithRetry() error {
	completeRetry := S3_RETRY_TIMES
	for completeRetry > 0 {
		err := m.targetCli.CompleteMultiUpload(m.DstBucketName,
			m.GetObjectHandler.ObjectName, m.PartsInfo, m.UploadId)
		if err != nil {
			m.logger.Error("CompleteMultiUpload error occurred, retry",
				zap.String("UploadId", m.UploadId))
			completeRetry -= 1
			time.Sleep(time.Second * time.Duration((S3_RETRY_TIMES-completeRetry)*S3_RETRY_SLEEP_SECONDS))
			continue
		}
		m.logger.Info("CompleteMultiUpload successfully",
			zap.String("UploadId", m.UploadId))
		return nil
	}
	return &util.S3Error{
		Bucket:   m.DstBucketName,
		Key:      m.GetObjectHandler.ObjectName,
		Function: COMPLETED_MULTIPART_UPLOAD_FUNCTION,
	}
}

func (m *MultiPartUploadHandle) abortMultiUploadWithRetry() error {
	abortRetry := S3_RETRY_TIMES
	for abortRetry > 0 {
		err := m.targetCli.AbortMultiUpload(m.DstBucketName,
			m.GetObjectHandler.ObjectName, m.UploadId)
		if err != nil {
			m.logger.Error("AbortMultiUpload failed, retry", zap.String("UploadId", m.UploadId))
			abortRetry -= 1
			time.Sleep(time.Second * time.Duration((S3_RETRY_TIMES-abortRetry)*S3_RETRY_SLEEP_SECONDS))
			continue
		}
		m.logger.Info("AbortMultiUpload successfully",
			zap.String("UploadId", m.UploadId))
		return nil
	}
	return &util.S3Error{
		Bucket:   m.DstBucketName,
		Key:      m.GetObjectHandler.ObjectName,
		Function: ABORT_MULTIUPLOAD_FUNCTION,
	}
}

var MultiPartUploadHandlePool = sync.Pool{
	New: func() interface{} {
		return &MultiPartUploadHandle{
			GetObjectHandler: nil,
			DstBucketName:    EMPTY_STRING,
			UploadId:         EMPTY_STRING,
			PartsInfo:        nil,
			NextPartNumber:   1,
			targetCli:        nil,
			logger:           nil,
		}
	},
}

func FetchMultiPartUploadHandle(getObjectHandler *GetObjectHandle,
	dstBucketName string, client *S3Client, logger *zap.Logger) *MultiPartUploadHandle {
	multiPartUploadHandle := MultiPartUploadHandlePool.Get().(*MultiPartUploadHandle)
	multiPartUploadHandle.GetObjectHandler = getObjectHandler
	multiPartUploadHandle.DstBucketName = dstBucketName
	multiPartUploadHandle.PartsInfo = make(map[int64]string)
	multiPartUploadHandle.targetCli = client
	multiPartUploadHandle.logger = logger
	return multiPartUploadHandle
}

func RestoreMultiPartUploadHandle(m *MultiPartUploadHandle) {
	m.Reset()
	MultiPartUploadHandlePool.Put(m)
}
