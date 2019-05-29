package backup

import (
	"net"
	"time"

	"s3assistant/common"
	"s3assistant/util"

	"go.uber.org/zap"
)

const (
	NET_ERROR_SLEEP_SECONDS = 2
	NET_ERROR_RETRY_TIMES   = 10
	INVALID_INT64_RETURN    = -100
	OK_STRING               = "OK"
	PROCESS_BACKUP_PREFIX   = "PROCESS_BACKUP"
	STRING_GLUE             = "#"
)

const (
	RPOPLPUSH_BACKUP_ITEM_FUNCTION      = "RPOPLPUSHBackupItem"
	HSET_BACKUP_ITEM_TIMESTAMP_FUNCTION = "HSETBackupItemTimeToKeepAlive"
	HGET_BACKUP_ITEM_TIMESTAMP_FUNCTION = "HGETBackupItemTimeToCheckAlive"
	HDEL_BACKUP_ITEM_TIMESTAMP_FUNCTION = "HDELBackupItemTimestamp"
	RPOP_LAST_BACKUP_ITEM_FUNCTION      = "RPOPLastItem"
	LRANGE_DST_QUEUE_FUNCTION           = "LRANGEDstQueue"
	LINDEX_LAST_BACKUP_ITEM_FUNCTION    = "LINDEXLastItem"
	LLEN_DST_QUEUE_FUNCTION             = "LLENDstQueue"
)

func MakeProcessBackupHashKey(bucketName string) string {
	return PROCESS_BACKUP_PREFIX + STRING_GLUE + bucketName
}

func MakeProcessBackupHashField(objectName, lastModified, operation string) string {
	return operation + STRING_GLUE + objectName + STRING_GLUE + lastModified
}

type BackupRedisHandler struct {
	*common.RedisBaseHandler
	logger *zap.Logger
}

func NewBackupRedisHandler(logger *zap.Logger, redisBaseHandler *common.RedisBaseHandler) *BackupRedisHandler {
	return &BackupRedisHandler{
		logger:           logger,
		RedisBaseHandler: redisBaseHandler,
	}
}

// RPOPLPUSHBackupItem pop backupItem from srcQueue into dstQueue
func (b *BackupRedisHandler) RPOPLPUSHBackupItem(srcQueue, dstQueue string) (string, error) {
	res, err := b.RPOPLPUSH(srcQueue, dstQueue)
	if err == nil {
		return res, nil
	}
	switch err.(type) {
	case *util.RedisError:
		err.(*util.RedisError).Function = RPOPLPUSH_BACKUP_ITEM_FUNCTION
	default:
		return res, err
	}
	return res, err
}

// HSETBackupItemTimeToKeepAlive to indicate task is alive, task is completed when currentTime equals to -1
func (b *BackupRedisHandler) HSETBackupItemTimeToKeepAlive(
	backupSourceInfo *BackupSourceInfo, currentTime string) (int64, error) {
	hashKey := MakeProcessBackupHashKey(backupSourceInfo.srcBucketName)
	hashField := MakeProcessBackupHashField(backupSourceInfo.objectName,
		backupSourceInfo.lastModified, backupSourceInfo.operation)
	res, err := b.HSET(hashKey, hashField, currentTime)
	if err == nil {
		return res, nil
	}
	switch err.(type) {
	case *util.RedisError:
		err.(*util.RedisError).Function = HSET_BACKUP_ITEM_TIMESTAMP_FUNCTION
	default:
		return res, err
	}
	return res, err
}

// HGETBackupItemTimeToCheckAlive to get backup item's process status
func (b *BackupRedisHandler) HGETBackupItemTimeToCheckAlive(backupSourceInfo *BackupSourceInfo) (string, error) {
	hashKey := MakeProcessBackupHashKey(backupSourceInfo.srcBucketName)
	hashField := MakeProcessBackupHashField(backupSourceInfo.objectName,
		backupSourceInfo.lastModified, backupSourceInfo.operation)
	res, err := b.HGET(hashKey, hashField)
	if err == nil {
		return res, nil
	}
	switch err.(type) {
	case *util.RedisError:
		err.(*util.RedisError).Function = HGET_BACKUP_ITEM_TIMESTAMP_FUNCTION
	default:
		return res, err
	}
	return res, err
}

func (b *BackupRedisHandler) HDELBackupItemTimestamp(backupSourceInfo *BackupSourceInfo) error {
	hashKey := MakeProcessBackupHashKey(backupSourceInfo.srcBucketName)
	hashField := MakeProcessBackupHashField(backupSourceInfo.objectName,
		backupSourceInfo.lastModified, backupSourceInfo.operation)
	_, err := b.HDEL(hashKey, hashField)
	if err == nil {
		return nil
	}
	switch err.(type) {
	case *util.RedisError:
		err.(*util.RedisError).Function = HDEL_BACKUP_ITEM_TIMESTAMP_FUNCTION
	default:
		return err
	}
	return err
}

// LINDEXLastItem to index the backup item to check if it has been completed
func (b *BackupRedisHandler) LINDEXLastItem(listName string, index int64) (string, error) {
	res, err := b.LINDEX(listName, index)
	if err == nil {
		return res, nil
	}
	switch err.(type) {
	case *util.RedisError:
		err.(*util.RedisError).Function = LINDEX_LAST_BACKUP_ITEM_FUNCTION
	default:
		return res, err
	}
	return res, err
}

func (b *BackupRedisHandler) LRANGEDstQueue(listName string, start, stop int64) ([]string, error) {
	res, err := b.LRANGE(listName, start, stop)
	if err == nil {
		if res == nil {
			return nil, nil
		}
		result := make([]string, len(res))
		for i := range res {
			switch res[i].(type) {
			case string:
				result[i] = res[i].(string)
			case []byte:
				result[i] = string(res[i].([]byte))
			default:
				return nil, &util.RedisError{
					Message:  "Switch lrange result failed.",
					Function: LRANGE_DST_QUEUE_FUNCTION,
				}
			}
		}
		return result, nil
	}
	switch err.(type) {
	case *util.RedisError:
		err.(*util.RedisError).Function = LRANGE_DST_QUEUE_FUNCTION
	default:
		return nil, err
	}
	return nil, err
}

// LLENDstQueue count the length of listName
func (b *BackupRedisHandler) LLENDstQueue(listName string) (int64, error) {
	res, err := b.LLEN(listName)
	if err == nil {
		return res, nil
	}
	switch err.(type) {
	case *util.RedisError:
		err.(*util.RedisError).Function = LLEN_DST_QUEUE_FUNCTION
	default:
		return res, err
	}
	return res, err
}

// RPOPLastItem to pop backup item when it has been completed
func (b *BackupRedisHandler) RPOPLastItem(listName string) error {
	_, err := b.RPOP(listName)
	if err == nil {
		return nil
	}
	switch err.(type) {
	case *util.RedisError:
		err.(*util.RedisError).Function = RPOP_LAST_BACKUP_ITEM_FUNCTION
	default:
		return err
	}
	return err
}

func (b *BackupRedisHandler) hsetBackupItemTimeToKeepAliveWithRetry(backupSourceInfo *BackupSourceInfo, currentTime string) bool {
	var hsetRetry int = NET_ERROR_RETRY_TIMES
	var err error
	for hsetRetry > 0 {
		_, err = b.HSETBackupItemTimeToKeepAlive(backupSourceInfo, currentTime)
		if err == nil {
			return true
		}
		switch err.(type) {
		case net.Error:
			hsetRetry -= 1
			b.logger.Error("HSETBackupItemTimeToKeepAlive net error occurred, retry",
				zap.Object("backupSourceInfo", backupSourceInfo),
				zap.String("currentTime", currentTime))
			time.Sleep(time.Second * time.Duration((NET_ERROR_RETRY_TIMES-hsetRetry)*NET_ERROR_SLEEP_SECONDS))
			break
		default:
			b.logger.Error("hsetBackupItemTimeToKeepAlive unexpected error occurred", zap.String("error", err.Error()))
			return false
		}
	}
	return false
}

func (b *BackupRedisHandler) hgetBackupItemTimeToCheckAliveWithRetry(backupSourceInfo *BackupSourceInfo) (string, bool) {
	var hgetRetry int = NET_ERROR_RETRY_TIMES
	var hgetResp string
	var err error
	for hgetRetry > 0 {
		hgetResp, err = b.HGETBackupItemTimeToCheckAlive(backupSourceInfo)
		if err == nil {
			return hgetResp, true
		}
		switch err.(type) {
		case net.Error:
			hgetRetry -= 1
			b.logger.Error("HGETBackupItemTimeToCheckAlive net error occurred, retry",
				zap.Object("backupSourceInfo", backupSourceInfo))
			time.Sleep(time.Second * time.Duration((NET_ERROR_RETRY_TIMES-hgetRetry)*NET_ERROR_SLEEP_SECONDS))
			break
		default:
			b.logger.Error("HGETBackupItemTimeToCheckAlive unexpected error occurred", zap.String("error", err.Error()))
			return common.EMPTY_STRING, false
		}
	}
	return common.EMPTY_STRING, false
}

func (b *BackupRedisHandler) hdelBackupItemTimestampWithRetry(backupSourceInfo *BackupSourceInfo) bool {
	var hdelRetry int = NET_ERROR_RETRY_TIMES
	var err error
	for hdelRetry > 0 {
		err = b.HDELBackupItemTimestamp(backupSourceInfo)
		if err == nil {
			return true
		}
		switch err.(type) {
		case net.Error:
			hdelRetry -= 1
			b.logger.Error("HDELBackupItemTimestamp net error occurred, retry",
				zap.Object("backupSourceInfo", backupSourceInfo))
			time.Sleep(time.Second * time.Duration((NET_ERROR_RETRY_TIMES-hdelRetry)*NET_ERROR_SLEEP_SECONDS))
			break
		default:
			b.logger.Error("HDELBackupItemTimestamp unexpected error occurred", zap.String("error", err.Error()))
			return false
		}
	}
	return false
}

func (b *BackupRedisHandler) lindexLastItemWithRetry(listName string, index int64) (string, bool) {
	var lindexRetry int = NET_ERROR_RETRY_TIMES
	var lindexResp string
	var err error
	for lindexRetry > 0 {
		lindexResp, err = b.LINDEXLastItem(listName, index)
		if err == nil {
			return lindexResp, true
		}
		switch err.(type) {
		case net.Error:
			lindexRetry -= 1
			b.logger.Error("LINDEXLastItem net error occurred, retry", zap.Int64("index", index))
			time.Sleep(time.Second * time.Duration((NET_ERROR_RETRY_TIMES-lindexRetry)*NET_ERROR_SLEEP_SECONDS))
			break
		default:
			b.logger.Error("LINDEXLastItem unexpected error occurred", zap.String("error", err.Error()))
			return common.EMPTY_STRING, false
		}
	}
	return common.EMPTY_STRING, false
}

func (b *BackupRedisHandler) lrangeDstQueueWithRetry(listName string, start, stop int64) ([]string, bool) {
	var lrangeRetry int = NET_ERROR_RETRY_TIMES
	var lrangeResp []string
	var err error
	for lrangeRetry > 0 {
		lrangeResp, err = b.LRANGEDstQueue(listName, start, stop)
		if err == nil {
			return lrangeResp, true
		}
		switch err.(type) {
		case net.Error:
			lrangeRetry -= 1
			b.logger.Error("LRANGEDstQueue net error occurred, retry",
				zap.String("dstQueue", listName),
				zap.Int64("startIndex", start),
				zap.Int64("stopIndex", stop))
			time.Sleep(time.Second * time.Duration((NET_ERROR_RETRY_TIMES*lrangeRetry)*NET_ERROR_SLEEP_SECONDS))
			break
		default:
			b.logger.Error("LRANGEDstQueue unexpected error occurred", zap.String("error", err.Error()))
			return nil, false
		}
	}
	return nil, false
}

func (b *BackupRedisHandler) llenDstQueueWithRetry(listName string) (int64, bool) {
	var llenRetry int = NET_ERROR_RETRY_TIMES
	var llenResp int64
	var err error
	for llenRetry > 0 {
		llenResp, err = b.LLENDstQueue(listName)
		if err == nil {
			return llenResp, true
		}
		switch err.(type) {
		case net.Error:
			llenRetry -= 1
			b.logger.Error("llenDstQueue net error occurred, retry",
				zap.String("dstQueue", listName))
			time.Sleep(time.Second * time.Duration((NET_ERROR_RETRY_TIMES*llenRetry)*NET_ERROR_SLEEP_SECONDS))
			break
		default:
			b.logger.Error("llenDstQueue unexpected error occurred", zap.String("error", err.Error()))
			return INVALID_INT64_RETURN, false
		}
	}
	return INVALID_INT64_RETURN, false
}
