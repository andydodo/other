package backup

import (
	"fmt"
	"strconv"
	"strings"
	"sync"

	"s3assistant/common"
	"s3assistant/util"

	"go.uber.org/zap/zapcore"
)

const (
	CHECKER_QUEUE_SUFFIX = "checker"
	MAX_IDLE_COUNT       = 1024
	LOG_IDLE_THRESHOLD   = 20
)

type BackupCommonMethodWrapper struct {
	// implement the common method used by components of backup
}

func (b *BackupCommonMethodWrapper) makeDstQueuePrefix(srcQueuePrefix string) string {
	return srcQueuePrefix + STRING_GLUE + CHECKER_QUEUE_SUFFIX
}

func (b *BackupCommonMethodWrapper) parseBackupSourceItem(backupSourceItem string) (*BackupChannelInfo, error) {
	backupChannelInfo := fetchBackupChannelInfo()
	// Format: "delete#bucketName#objcetName#lastModified"
	//         "put#bucketName#objcetName#lastModified#objectSize"
	fields := strings.Split(backupSourceItem, STRING_GLUE)
	if len(fields) == 4 && fields[0] == DELETE_OPERATION {
		backupChannelInfo.backupSourceInfo.operation = fields[0]
		backupChannelInfo.backupSourceInfo.srcBucketName = fields[1]
		backupChannelInfo.backupSourceInfo.objectName = fields[2]
		backupChannelInfo.backupSourceInfo.lastModified = fields[3]
		return backupChannelInfo, nil
	}
	if len(fields) == 5 && fields[0] == PUT_OPERATION {
		backupChannelInfo.backupSourceInfo.operation = fields[0]
		backupChannelInfo.backupSourceInfo.srcBucketName = fields[1]
		backupChannelInfo.backupSourceInfo.objectName = fields[2]
		backupChannelInfo.backupSourceInfo.lastModified = fields[3]
		objSize, err := strconv.ParseUint(fields[4], 10, 64)
		if err != nil {
			return nil, &util.ParseError{Message: fmt.Sprintf("backupSourceInfo with wrong object size format: %s", backupSourceItem)}
		}
		backupChannelInfo.backupSourceInfo.objectSize = objSize
		return backupChannelInfo, nil
	}
	return nil, &util.ParseError{Message: fmt.Sprintf("backupSourceItem with wrong format: %s", backupSourceItem)}
}

type BackupChannelInfo struct {
	dispatcherID     int
	backupSourceInfo *BackupSourceInfo
}

func (b *BackupChannelInfo) Reset() {
	b.dispatcherID = -1
	b.backupSourceInfo.Reset()
}

type BackupSourceInfo struct {
	operation, srcBucketName, dstBucketName, objectName, lastModified string
	objectSize                                                        uint64
}

func (b *BackupSourceInfo) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddString("operation", b.operation)
	enc.AddString("srcBucketName", b.srcBucketName)
	enc.AddString("dstBucketName", b.dstBucketName)
	enc.AddString("objectName", b.objectName)
	enc.AddString("lastModified", b.lastModified)
	enc.AddUint64("objectSize", b.objectSize)
	return nil
}

func (b *BackupSourceInfo) Reset() {
	b.operation = common.EMPTY_STRING
	b.srcBucketName = common.EMPTY_STRING
	b.dstBucketName = common.EMPTY_STRING
	b.objectName = common.EMPTY_STRING
	b.lastModified = common.EMPTY_STRING
	b.objectSize = 0
}

var BackupChannelInfoPool = sync.Pool{
	New: func() interface{} {
		return &BackupChannelInfo{
			dispatcherID: -1,
			backupSourceInfo: &BackupSourceInfo{
				operation:     common.EMPTY_STRING,
				srcBucketName: common.EMPTY_STRING,
				dstBucketName: common.EMPTY_STRING,
				objectName:    common.EMPTY_STRING,
				lastModified:  common.EMPTY_STRING,
				objectSize:    0,
			},
		}
	},
}

func fetchBackupChannelInfo() *BackupChannelInfo {
	backupChannelInfo := BackupChannelInfoPool.Get().(*BackupChannelInfo)
	return backupChannelInfo
}

func restoreBackupChannelInfo(b *BackupChannelInfo) {
	b.Reset()
	BackupChannelInfoPool.Put(b)
}
