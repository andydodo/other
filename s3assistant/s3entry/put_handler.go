package s3entry

import (
	"s3assistant/util"
	"sync"
	"time"

	"go.uber.org/zap"
)

// PutHandle used to put object to destination
type PutHandle struct {
	GetObjectHandler *GetObjectHandle
	DstBucketName    string
	targetCli        *S3Client
	logger           *zap.Logger
}

func (p *PutHandle) clearFd() {
	p.GetObjectHandler.clearFd()
}

func (p *PutHandle) Reset() {
	p.DstBucketName = EMPTY_STRING
	p.targetCli = nil
	p.logger = nil
}

func (p *PutHandle) PutObjectWithRetry() error {
	putObjectRetry := S3_RETRY_TIMES
	for putObjectRetry > 0 {
		p.GetObjectHandler.FdHandler.Fd.Seek(0, 0)
		err := p.targetCli.PutObject(
			p.GetObjectHandler.FdHandler.Fd,
			p.DstBucketName,
			p.GetObjectHandler.ObjectName)
		if err != nil {
			p.logger.Error("PutObject error occurred, retry",
				zap.String("error", err.Error()))
			putObjectRetry -= 1
			time.Sleep(time.Second * time.Duration((S3_RETRY_TIMES-putObjectRetry)*S3_RETRY_SLEEP_SECONDS))
			continue
		}
		p.logger.Info("PutObject successfully")
		return nil
	}
	return &util.S3Error{
		Bucket:   p.DstBucketName,
		Key:      p.GetObjectHandler.ObjectName,
		Function: PUT_OBJECT_FUNCTION,
	}
}

var PutHandlePool = sync.Pool{
	New: func() interface{} {
		return &PutHandle{
			GetObjectHandler: nil,
			DstBucketName:    EMPTY_STRING,
			targetCli:        nil,
			logger:           nil,
		}
	},
}

func FetchPutHandle(getObjectHander *GetObjectHandle,
	dstBucketName string, targetCli *S3Client, logger *zap.Logger) *PutHandle {
	putHandler := PutHandlePool.Get().(*PutHandle)
	putHandler.GetObjectHandler = getObjectHander
	putHandler.DstBucketName = dstBucketName
	putHandler.targetCli = targetCli
	putHandler.logger = logger
	return putHandler
}

func RestorePutHandle(p *PutHandle) {
	p.Reset()
	PutHandlePool.Put(p)
}
