package s3entry

import (
	"s3assistant/util"
	"sync"
	"time"

	"go.uber.org/zap"
)

type DeleteHandle struct {
	BucketName string
	ObjectName string
	targetCli  *S3Client
	logger     *zap.Logger
}

func (d *DeleteHandle) reset() {
	d.BucketName = EMPTY_STRING
	d.ObjectName = EMPTY_STRING
	d.targetCli = nil
	d.logger = nil
}

func (d *DeleteHandle) DeleteObjectWithRetry() error {
	deleteRetry := S3_RETRY_TIMES
	for deleteRetry > 0 {
		err := d.targetCli.DeleteObject(d.BucketName, d.ObjectName)
		if err != nil {
			d.logger.Error("DeleteObject error occurred, retry",
				zap.String("error", err.Error()))
			deleteRetry -= 1
			time.Sleep(time.Second * time.Duration((S3_RETRY_TIMES-deleteRetry)*S3_RETRY_SLEEP_SECONDS))
			continue
		}
		d.logger.Info("DeleteObject successfully")
		return nil
	}
	return &util.S3Error{
		Bucket:   d.BucketName,
		Key:      d.ObjectName,
		Function: DELETE_OBJECT_FUNCTION,
	}
}

var DeleteHandlePool = sync.Pool{
	New: func() interface{} {
		return &DeleteHandle{
			BucketName: EMPTY_STRING,
			ObjectName: EMPTY_STRING,
			targetCli:  nil,
			logger:     nil,
		}
	},
}

func FetchDeleteHandle(bucketName, objectName string,
	targetCli *S3Client, logger *zap.Logger) *DeleteHandle {
	deleteHandle := DeleteHandlePool.Get().(*DeleteHandle)
	deleteHandle.BucketName = bucketName
	deleteHandle.ObjectName = objectName
	deleteHandle.targetCli = targetCli
	deleteHandle.logger = logger
	return deleteHandle
}

func RestoreDeleteHandle(d *DeleteHandle) {
	d.reset()
	DeleteHandlePool.Put(d)
}
