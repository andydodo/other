package asyncdelete

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"s3assistant/common"
	"s3assistant/util"

	"go.uber.org/zap"
)

const (
	BLOCK_REF_PREFIX = "BLOCK_REF"
	STRING_GLUE      = "#"

	// the following feilds transfer the original redis replies to distinguish different states
	NET_ERROR_STRING_RETURN = "NET_ERROR"
	NET_ERROR_INT64_RETURN  = -110
	INVALID_INT64_RETURN    = -10 // used to mark invalid redis reply because an error occurred.
	NOT_FOUND_INT64_RETURN  = -20 // used to mark empty redis reply without error.
)

const (
	SPOP_TEMP_FUNCTION     = "SPOPTempBlockKey"
	SCAN_TEMP_FUNCTION     = "SCANTempBlockSet"
	GET_BLOCK_REF_FUNCTION = "GETBlockRefKey"
	RPOP_DELETED_FUNCTION  = "RPOPDeletedBlockKey"
	INCREASE_REF_FUNCTION  = "IncreaseRef"
	DEL_REF_FUNCTION       = "DELREF"
	RPUSH_DELETED_FUNCTION = "RPUSHDeletedBlockKey"
)

func MakeBlockRefKey(blockKey string) string {
	return BLOCK_REF_PREFIX + STRING_GLUE + blockKey
}

type AsyncDeleteRedisHandler struct {
	*common.RedisBaseHandler
	logger                *zap.Logger
	blockReserverdTime    int64
	deletedBlockList      string
	tempBlockSetKeyPrefix string
}

func NewAsyncDeleteRedisHandler(logger *zap.Logger, redisBaseHandler *common.RedisBaseHandler,
	blockReserverdTime int64, deletedBlockList, tempBlockSetKeyPrefix string) *AsyncDeleteRedisHandler {
	return &AsyncDeleteRedisHandler{
		logger:                logger,
		RedisBaseHandler:      redisBaseHandler,
		blockReserverdTime:    blockReserverdTime,
		deletedBlockList:      deletedBlockList,
		tempBlockSetKeyPrefix: tempBlockSetKeyPrefix,
	}
}

func (a *AsyncDeleteRedisHandler) GetExpiredTempBlockSetKeys(cursor *string, count string) ([]string, error) {
	res, err := a.SCANTempBlockSet(*cursor, a.tempBlockSetKeyPrefix, count)
	if err != nil {
		return nil, err
	}

	if len(res) != 2 {
		return nil, &util.RedisError{
			Message:  "Scan reply error",
			Function: "ScanTempBlockSet",
		}
	}

	nextCursor, ok := res[0].([]byte)
	if !ok {
		return nil, &util.RedisError{
			Message:  "Scan reply error",
			Function: "ScanTempBlockSet",
		}
	}
	*cursor = string(nextCursor)

	if res[1] == nil {
		return nil, nil
	}

	blockSet, ok := res[1].([][]byte)
	if !ok {
		return nil, &util.RedisError{
			Message:  "Scan reply error",
			Function: "ScanTempBlockSet",
		}
	}

	var result []string
	for _, tempSetKey := range blockSet {
		ok, err := a.isTempSetExpired(string(tempSetKey))
		if err != nil {
			a.logger.Error(err.Error())
			continue
		}
		if !ok {
			continue
		}
		result = append(result, string(tempSetKey))
	}
	return result, nil
}

func (a *AsyncDeleteRedisHandler) SCANTempBlockSet(cursor, prefix, count string) ([]interface{}, error) {
	res, err := a.SCAN(cursor, prefix, count)
	if err == nil {
		return res, nil
	}
	switch err.(type) {
	case *util.RedisError:
		err.(*util.RedisError).Function = SCAN_TEMP_FUNCTION
	default:
		return res, err
	}
	return res, err
}

func (a *AsyncDeleteRedisHandler) SPOPTempBlockKey(tempSetKey string) (string, error) {
	res, err := a.SPOP(tempSetKey)
	if err == nil {
		return res, nil
	}
	switch err.(type) {
	case *util.RedisError:
		err.(*util.RedisError).Function = SPOP_TEMP_FUNCTION
	default:
		return res, err
	}
	return res, err
}

func (a *AsyncDeleteRedisHandler) GetDeletedBlockKey() (string, error) {
	// 1. RPOP
	blockKey, err := a.RPOPDeletedBlockKey()
	if err != nil {
		return common.EMPTY_STRING, err
	}

	if blockKey == common.EMPTY_STRING {
		return common.EMPTY_STRING, nil
	}

	// 2. Check time
	realKey, err := a.parseBlockKeyAndCheckExpired(blockKey)
	if err != nil {
		return common.EMPTY_STRING, &util.RedisError{
			Message:  fmt.Sprintf("blockKey: %s, err: %s", blockKey, err.Error()),
			Function: "parseBlockKeyAndCheckExpired",
		}
	}

	// 3. PUSH back only when blockKey is valid and no error occurred.
	if realKey == common.EMPTY_STRING {
		err = a.RPUSHDeletedBlockKey(blockKey)
		if err != nil {
			a.logger.Error("RPUSHDeletedBlockKey failed", zap.String("blockKey", blockKey))
			return common.EMPTY_STRING, err
		}
	}
	return realKey, nil
}

func (a *AsyncDeleteRedisHandler) RPOPDeletedBlockKey() (string, error) {
	res, err := a.RPOP(a.deletedBlockList)
	if err == nil {
		return res, nil
	}
	switch err.(type) {
	case *util.RedisError:
		err.(*util.RedisError).Function = RPOP_DELETED_FUNCTION
	default:
		return res, err
	}
	return res, err
}

func (a *AsyncDeleteRedisHandler) RPUSHDeletedBlockKey(key string) error {
	err := a.RPUSH(a.deletedBlockList, key)
	if err == nil {
		return nil
	}
	switch err.(type) {
	case *util.RedisError:
		err.(*util.RedisError).Function = RPUSH_DELETED_FUNCTION
	default:
		return err
	}
	return err
}

func (a *AsyncDeleteRedisHandler) GETBlockRefKey(blockKey string) (int64, error) {
	res, err := a.GET(MakeBlockRefKey(blockKey))
	if err == nil {
		if res != common.EMPTY_STRING {
			r, err := strconv.ParseInt(res, 10, 64)
			if err != nil {
				return INVALID_INT64_RETURN, err
			}
			return r, nil
		}
		return NOT_FOUND_INT64_RETURN, nil
	}
	switch err.(type) {
	case *util.RedisError:
		err.(*util.RedisError).Function = GET_BLOCK_REF_FUNCTION
	default:
		return INVALID_INT64_RETURN, err
	}
	return INVALID_INT64_RETURN, err
}

func (a *AsyncDeleteRedisHandler) IncreaseRef(blockKey string, num int64) (int64, bool) {
	res, err := a.INCRBY(MakeBlockRefKey(blockKey), num)
	if err == nil {
		return res, true
	}
	switch err.(type) {
	case net.Error:
		return NET_ERROR_INT64_RETURN, true
	case *util.RedisError:
		err.(*util.RedisError).Function = INCREASE_REF_FUNCTION
	default:
		return INVALID_INT64_RETURN, false
	}
	return INVALID_INT64_RETURN, false
}

func (a *AsyncDeleteRedisHandler) DelRef(blockKey string) (int64, error) {
	res, err := a.DEL(MakeBlockRefKey(blockKey))
	if err == nil {
		return res, nil
	}
	switch err.(type) {
	case *util.RedisError:
		err.(*util.RedisError).Function = DEL_REF_FUNCTION
	default:
		return INVALID_INT64_RETURN, err
	}
	return INVALID_INT64_RETURN, err
}

func (a *AsyncDeleteRedisHandler) isTempSetExpired(tempSetKey string) (bool, error) {
	fields := strings.Split(tempSetKey, STRING_GLUE)
	if len(fields) != 4 {
		return false, &util.ParseError{Message: fmt.Sprintf("tempSetKey with wrong fomat: %s", tempSetKey)}
	}

	timeStr := fields[len(fields)-1]
	timeStrLen := strings.Count(timeStr, "") - 1
	if timeStrLen < 6 {
		return false, &util.ParseError{Message: fmt.Sprintf("tempSetKey with wrong fomat: %s", tempSetKey)}
	}
	createTime, err := strconv.ParseInt(timeStr[0:timeStrLen-6], 10, 64)
	if err == nil {
		timeDiff := time.Now().Unix() - createTime
		if timeDiff > a.blockReserverdTime {
			return true, nil
		}
	}
	return false, err
}

func (a *AsyncDeleteRedisHandler) parseBlockKeyAndCheckExpired(blockKey string) (string, error) {
	// Format: realBlockKey#timestamp
	fields := strings.Split(blockKey, STRING_GLUE)

	if len(fields) != 2 {
		return common.EMPTY_STRING, &util.ParseError{Message: fmt.Sprintf("blockKey with wrong format: %s", blockKey)}
	}

	timeStr := fields[len(fields)-1]
	timeStrLen := strings.Count(timeStr, "") - 1
	if timeStrLen < 6 {
		return common.EMPTY_STRING, &util.ParseError{Message: fmt.Sprintf("blockKey with wrong format: %s", blockKey)}
	}
	// int64
	createTime, err := strconv.ParseInt(timeStr[0:timeStrLen-6], 10, 64)
	if err == nil {
		timeDiff := time.Now().Unix() - createTime
		if timeDiff > a.blockReserverdTime {
			return fields[0], nil
		} else {
			return common.EMPTY_STRING, nil
		}
	}
	return common.EMPTY_STRING, err
}

func (a *AsyncDeleteRedisHandler) getBlockRefWithRetry(blockKey string) (int64, bool) {
	var getRetry int = NET_ERROR_RETRY_TIMES
	for getRetry > 0 {
		getResp, err := a.GETBlockRefKey(blockKey)
		if err == nil {
			return getResp, true
		}
		switch err.(type) {
		case net.Error:
			a.logger.Warn("GETBlockRefKey failed, retry", zap.String("blockKey", blockKey))
			getRetry -= 1
			time.Sleep(time.Second * time.Duration((NET_ERROR_RETRY_TIMES-getRetry)*NET_ERROR_SLEEP_SECONDS))
			break
		default:
			a.logger.Error("GETBlockRefKey unexpected errror occurred", zap.String("error", err.Error()))
			return INVALID_INT64_RETURN, false
		}
	}
	return NET_ERROR_INT64_RETURN, true
}

func (a *AsyncDeleteRedisHandler) delRefWithRetry(blockKey string) (int64, bool) {
	var delRefRetry int = NET_ERROR_RETRY_TIMES
	for delRefRetry > 0 {
		delResp, err := a.DelRef(blockKey)
		if err == nil {
			return delResp, true
		}
		switch err.(type) {
		case net.Error:
			a.logger.Warn("DelRef failed, retry", zap.String("blockKey", blockKey))
			delRefRetry -= 1
			time.Sleep(time.Second * time.Duration((NET_ERROR_RETRY_TIMES-delRefRetry)*NET_ERROR_SLEEP_SECONDS))
			break
		default:
			a.logger.Error("DelRef unexpected errror occurred", zap.String("error", err.Error()))
			return INVALID_INT64_RETURN, false
		}
	}
	return NET_ERROR_INT64_RETURN, true
}
