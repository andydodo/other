package producer

import (
	"s3assistant/common"
	"s3assistant/migration"
	"s3assistant/util"
)

type metaRedisHandler struct {
	metaRedis *common.RedisBaseHandler // Redis used for fetch source meta information
}

func NewMetaRedisHandler(metaRedis *common.RedisBaseHandler) *metaRedisHandler {
	return &metaRedisHandler{
		metaRedis: metaRedis,
	}
}

// ValidateRedisConnection check the connectivity for redis connections
func (m *metaRedisHandler) ValidateRedisConnection() error {
	if m.metaRedis != nil {
		if err := m.metaRedis.ValidateRedisConnection(); err != nil {
			return err
		}
	}
	return nil
}

func (m *metaRedisHandler) SCANMigrationItems(
	setKey string, cursor *string, pattern, count string) ([]string, error) {
	var res []interface{}
	var err error
	res, err = m.metaRedis.SSCAN(setKey, *cursor, pattern, count)
	if err == nil {
		if len(res) != 2 {
			return nil, &util.RedisError{}
		}
		// update cursor
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
