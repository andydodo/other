package broker

import (
	"fmt"
	"net"
	"s3assistant/common"
	"s3assistant/migration"
	"s3assistant/util"
	"time"

	"go.uber.org/zap"
)

type BrokerRedisHandler struct {
	brokerRedis *common.RedisBaseHandler // Redis used for communicating and controlling
}

func NewBrokerRedisHandler(brokerRedis *common.RedisBaseHandler) *BrokerRedisHandler {
	return &BrokerRedisHandler{
		brokerRedis: brokerRedis,
	}
}

// ValidateRedisConnection check the connectivity for redis connections
func (b *BrokerRedisHandler) ValidateRedisConnection() error {
	if b.brokerRedis != nil {
		if err := b.brokerRedis.ValidateRedisConnection(); err != nil {
			return err
		}
	}
	return nil
}

// LPUSHMigrationItem push item to broker queue
func (b *BrokerRedisHandler) LPUSHMigrationItem(queue string, items ...string) error {
	err := b.brokerRedis.LPUSH(queue, items...)
	if err == nil {
		return nil
	}
	switch err.(type) {
	case *util.RedisError:
		err.(*util.RedisError).Function = migration.LPUSH_MIGRATE_ITEM_FUNCTION
	default:
		return err
	}
	return err
}

// RPOPMigrationItem pop item from broker queue
func (b *BrokerRedisHandler) RPOPMigrationItem(queue string) (string, error) {
	res, err := b.brokerRedis.RPOP(queue)
	if err == nil {
		return res, nil
	}
	switch err.(type) {
	case *util.RedisError:
		err.(*util.RedisError).Function = migration.RPOP_MIGRATE_ITEM_FUNCTION
	default:
		return common.EMPTY_STRING, err
	}
	return common.EMPTY_STRING, err
}

// SCANMigrationItems scan items form meta redis or broker redis
// which decided by flag
func (b *BrokerRedisHandler) SCANMigrationItems(setKey string,
	cursor *string, pattern, count string) ([]string, error) {
	var res []interface{}
	var err error
	res, err = b.brokerRedis.SSCAN(setKey, *cursor, pattern, count)
	if err == nil {
		if len(res) != 2 {
			return nil, &util.RedisError{}
		}
		// update cursor for next scan operation
		*cursor = string(res[0].([]byte))
		if res[1] == nil {
			return nil, nil
		}
		resVal := res[1].([]interface{})
		result := make([]string, len(resVal))
		for i := range resVal {
			switch resVal[i].(type) {
			case string:
				result[i] = resVal[i].(string)
			case []byte:
				result[i] = string(resVal[i].([]byte))
			default:
				return nil, &util.RedisError{
					Message:  "Switch sscan result failed.",
					Function: migration.SCAN_MIGRATE_ITEMS_FUNCTION,
				}
			}
		}
		return result, nil
	}
	switch err.(type) {
	case *util.RedisError:
		err.(*util.RedisError).Function = migration.SCAN_MIGRATE_ITEMS_FUNCTION
	default:
		return nil, err
	}
	return nil, err
}

// llenQueue fetch the size of queue right now
func (b *BrokerRedisHandler) llenQueue(queue string) (int64, error) {
	res, err := b.brokerRedis.LLEN(queue)
	if err == nil {
		return res, nil
	}
	switch err.(type) {
	case *util.RedisError:
		err.(*util.RedisError).Function = migration.LLEN_QUEUE
	default:
		return common.REDIS_REPLY_INVALID_INT64_RETURN, err
	}
	return common.REDIS_REPLY_INVALID_INT64_RETURN, err
}

// setScanCursor set current scan cursor to broker redis
func (b *BrokerRedisHandler) setScanCursor(stringKey, cursor string) error {
	_, err := b.brokerRedis.SET(stringKey, cursor)
	if err == nil {
		return nil
	}
	switch err.(type) {
	case *util.RedisError:
		err.(*util.RedisError).Function = migration.SET_SCAN_CURSOR_FUNCTION
	default:
		return err
	}
	return err
}

// getScanCursor get last scan cursor to continue
func (b *BrokerRedisHandler) getScanCursor(stringKey string) (string, error) {
	res, err := b.brokerRedis.GET(stringKey)
	if err == nil {
		return res, nil
	}
	switch err.(type) {
	case *util.RedisError:
		err.(*util.RedisError).Function = migration.GET_SCAN_CURSOR_FUNCTION
	default:
		return common.EMPTY_STRING, err
	}
	return common.EMPTY_STRING, err
}

// delScanCursor delete the scan cursor when all items have been traversed
func (b *BrokerRedisHandler) delScanCursor(stringKey string) error {
	_, err := b.brokerRedis.DEL(stringKey)
	if err == nil {
		return nil
	}
	switch err.(type) {
	case *util.RedisError:
		err.(*util.RedisError).Function = migration.DEL_SCAN_CURSOR_FUNCTION
	}
	return err
}

// saddCompletedMigrationItems store the items which have been processed successfully by worker
func (b *BrokerRedisHandler) saddCompletedMigrationItems(
	setKey string, item *migration.MigrationItem) (int64, error) {
	res, err := b.brokerRedis.SADD(setKey, item.ObjectName)
	if err == nil {
		return res, nil
	}
	switch err.(type) {
	case *util.RedisError:
		err.(*util.RedisError).Function = migration.SADD_COMPLETED_MIGRATE_ITEMS_FUNCTION
	default:
		return common.REDIS_REPLY_INVALID_INT64_RETURN, err
	}
	return common.REDIS_REPLY_INVALID_INT64_RETURN, err
}

// saddAddedMigrationItems store the items which have been added to broker waitting for processing
func (b *BrokerRedisHandler) saddAddedMigrationItems(
	setKey string, objectNames ...string) (int64, error) {
	res, err := b.brokerRedis.SADD(setKey, objectNames...)
	if err == nil {
		return res, nil
	}
	switch err.(type) {
	case *util.RedisError:
		err.(*util.RedisError).Function = migration.SADD_ADDED_MIGRATE_ITEMS_FUNCTION
	default:
		return common.REDIS_REPLY_INVALID_INT64_RETURN, err
	}
	return common.REDIS_REPLY_INVALID_INT64_RETURN, err
}

// sdiffStoreAddedAddCompletedMigrationItems sdiff the added and completed set
// to check if there exists failed items
func (b *BrokerRedisHandler) sdiffStoreAddedAddCompletedMigrationItems(
	sdiffStoreSetKey, addedSetKey, completedSetKey string) (int64, error) {
	res, err := b.brokerRedis.SDIFFSTORE(sdiffStoreSetKey, addedSetKey, completedSetKey)
	if err == nil {
		return res, nil
	}
	switch err.(type) {
	case *util.RedisError:
		err.(*util.RedisError).Function = migration.SDIFFSTORE_ADDED_AND_COMPLETED_MIGRATE_ITEMS_FUNCTION
	default:
		return common.REDIS_REPLY_INVALID_INT64_RETURN, err
	}
	return common.REDIS_REPLY_INVALID_INT64_RETURN, err
}

func (b *BrokerRedisHandler) SaddAddedMigrationItemsWithRetry(
	setKey string, objectNames ...string) (error, bool) {
	var saddRetry int = migration.NET_ERROR_RETRY_TIMES
	var err error
	for saddRetry > 0 {
		_, err = b.saddAddedMigrationItems(setKey, objectNames...)
		if err == nil {
			return nil, true
		}
		fmt.Printf("sadd failed...\n")
		switch err.(type) {
		case net.Error:
			saddRetry -= 1
			migration.Logger.Error("saddAddedMigrationItemsWithRetry net error occurred, retry",
				zap.String("Key", setKey))
			time.Sleep(time.Second * time.Duration((migration.NET_ERROR_RETRY_TIMES*saddRetry)*
				migration.NET_ERROR_SLEEP_SECONDS))
			break
		default:
			migration.Logger.Error("saddAddedMigrationItemsWithRetry unexpected error occurred",
				zap.String("error", err.Error()))
			return err, false
		}
	}
	return err, true
}

func (b *BrokerRedisHandler) SaddCompletedMigrationItemsWithRetry(
	setKey string, item *migration.MigrationItem) (error, bool) {
	var saddRetry int = migration.NET_ERROR_RETRY_TIMES
	var err error
	for saddRetry > 0 {
		_, err = b.saddCompletedMigrationItems(setKey, item)
		if err == nil {
			return nil, true
		}
		switch err.(type) {
		case net.Error:
			saddRetry -= 1
			migration.Logger.Error("saddCompletedMigrationItemsWithRetry net error occurred, retry",
				zap.Object("MigrationItemInfo", item))
			time.Sleep(time.Second * time.Duration((migration.NET_ERROR_RETRY_TIMES*saddRetry)*
				migration.NET_ERROR_SLEEP_SECONDS))
			break
		default:
			migration.Logger.Error("saddCompletedMigrationItemsWithRetry nexpected error occurred",
				zap.String("error", err.Error()))
			return err, false
		}
	}
	return err, true
}

func (b *BrokerRedisHandler) LLenQueueWithRetry(queue string) (int64, bool) {
	var llenRetry int = migration.NET_ERROR_RETRY_TIMES
	var llenResp int64
	var err error
	for llenRetry > 0 {
		llenResp, err = b.llenQueue(queue)
		if err == nil {
			return llenResp, true
		}
		switch err.(type) {
		case net.Error:
			llenRetry -= 1
			migration.Logger.Error("LLenQueueWithRetry net error occurred, retry",
				zap.String("queue", queue))
			time.Sleep(time.Second * time.Duration((migration.NET_ERROR_RETRY_TIMES*llenRetry)*
				migration.NET_ERROR_SLEEP_SECONDS))
			break
		default:
			migration.Logger.Error("LLenQueueWithRetry unexpected error occurred",
				zap.String("error", err.Error()))
			return common.REDIS_REPLY_INVALID_INT64_RETURN, false
		}
	}
	return common.REDIS_REPLY_INVALID_INT64_RETURN, false
}

func (b *BrokerRedisHandler) GetScanCursorWithRetry(stringKey string) (string, bool) {
	var getRetry int = migration.NET_ERROR_RETRY_TIMES
	var getResp string
	var err error
	for getRetry > 0 {
		getResp, err = b.getScanCursor(stringKey)
		if err == nil {
			return getResp, true
		}
		switch err.(type) {
		case net.Error:
			getRetry -= 1
			migration.Logger.Error("getScanCursorWithRetry net error occurred, retry",
				zap.String("key", stringKey))
			time.Sleep(time.Second * time.Duration((migration.NET_ERROR_RETRY_TIMES*getRetry)*
				migration.NET_ERROR_SLEEP_SECONDS))
			break
		default:
			migration.Logger.Error("getScanCursorWithRetry unexpected error occurred",
				zap.String("error", err.Error()))
			return common.EMPTY_STRING, false
		}
	}
	return common.EMPTY_STRING, false
}

func (b *BrokerRedisHandler) SetScanCursorWithRetry(stringKey, cursor string) (error, bool) {
	var setRetry int = migration.NET_ERROR_RETRY_TIMES
	var err error
	for setRetry > 0 {
		err = b.setScanCursor(stringKey, cursor)
		if err == nil {
			return nil, true
		}
		switch err.(type) {
		case net.Error:
			setRetry -= 1
			migration.Logger.Error("setScanCursorWithRetry net error occurred, retry",
				zap.String("key", stringKey),
				zap.String("cursor", cursor))
			time.Sleep(time.Second * time.Duration((migration.NET_ERROR_RETRY_TIMES*setRetry)*
				migration.NET_ERROR_SLEEP_SECONDS))
			break
		default:
			migration.Logger.Error("setScanCursorWithRetry unexpected error occurred",
				zap.String("error", err.Error()))
			return err, false
		}
	}
	return err, true
}

func (b *BrokerRedisHandler) DelScanCursorWithRetry(stringKey string) (error, bool) {
	var delRetry int = migration.NET_ERROR_RETRY_TIMES
	var err error
	for delRetry > 0 {
		err = b.delScanCursor(stringKey)
		if err == nil {
			return nil, true
		}
		switch err.(type) {
		case net.Error:
			delRetry -= 1
			migration.Logger.Error("DelScanCursorWithRetry net error occurred, retry",
				zap.String("key", stringKey))
			time.Sleep(time.Second * time.Duration((migration.NET_ERROR_RETRY_TIMES*delRetry)*
				migration.NET_ERROR_SLEEP_SECONDS))
			break
		default:
			migration.Logger.Error("DelScanCursorWithRetry unexpected error occurred",
				zap.String("error", err.Error()))
			return err, false
		}
	}
	return err, true
}

func (b *BrokerRedisHandler) SdiffStoreAddedAndCompletedMigrationItemsWithRetry(
	sdiffStoreSetKey, addedSetKey, completedSetKey string) (int64, bool) {
	var sdiffStoreRetry int = migration.NET_ERROR_RETRY_TIMES
	var sdiffStoreResp int64
	var err error
	for sdiffStoreRetry > 0 {
		sdiffStoreResp, err = b.sdiffStoreAddedAddCompletedMigrationItems(
			sdiffStoreSetKey, addedSetKey, completedSetKey)
		if err == nil {
			return sdiffStoreResp, true
		}
		switch err.(type) {
		case net.Error:
			sdiffStoreRetry -= 1
			migration.Logger.Error("sdiffStoreAddedAndCompletedMigrationItemsWithRetry net error occurred, retry",
				zap.String("addedSetKey", addedSetKey),
				zap.String("completedSetKey", completedSetKey),
				zap.String("sdiffStoreSetKey", addedSetKey))
			time.Sleep(time.Second * time.Duration((migration.NET_ERROR_RETRY_TIMES*sdiffStoreRetry)*
				migration.NET_ERROR_SLEEP_SECONDS))
			break
		default:
			migration.Logger.Error("sdiffStoreAddedAndCompletedMigrationItemsWithRetry unexpected error occurred",
				zap.String("error", err.Error()))
			return common.REDIS_REPLY_INVALID_INT64_RETURN, false
		}
	}
	return common.REDIS_REPLY_INVALID_INT64_RETURN, true
}
