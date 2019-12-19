package scan

import (
	"encoding/json"
	"strconv"
	"strings"

	"github.com/andy/pika-scan/common"
	js "github.com/andy/pika-scan/json"
	"github.com/andy/pika-scan/util"
	"go.uber.org/zap"
)

const (
	SACN_BUCKET_OBJS = "SCANBucketObjs"
)

type FidInfo struct {
	bucket string
	object string
	index  int64
	fids   []string
}

type ScanConf struct {
	RedisPoolConfBase *common.RedisPoolConfBase `yaml:"redis_pool_conf"`
	Bucket            string                    `yaml:"bucket"`
	LogDir            string                    `yaml:"log_dir"`
	LogLevel          int                       `yaml:"log_level"`
}

/*
type ScanHandler struct {
	//logger *zap.Logger
	config *ScanConf
}
*/

type ScanRedisHandler struct {
	redisBaseHandler *common.RedisBaseHandler // the handler used to communicate with assistant meta
	logger           *zap.Logger
	Config           *ScanConf
}

/*
func NewScanHandler(ScanConf *ScanConf) *ScanHandler {
	return &ScanHandler{
		config: ScanConf,
	}
}
*/

func NewScanRedisHandler(logger *zap.Logger, redisBaseHandler *common.RedisBaseHandler, conf *ScanConf) *ScanRedisHandler {
	return &ScanRedisHandler{
		redisBaseHandler: redisBaseHandler,
		logger:           logger,
		Config:           conf,
		//todo: make channel use for concurrent
	}
}

func (a *ScanRedisHandler) SCANBucketObjs(pattern, count string, ch chan<- string) ([]string, error) {
	var (
		objects []string = nil
		err     error    = nil
		cursor  string   = "0"
	)

	op := func(cursor *string) ([]string, error) {
		var res []interface{}
		res, err = a.redisBaseHandler.SCAN(*cursor, pattern, count)
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
						Message:  "Switch scan result failed.",
						Function: SACN_BUCKET_OBJS,
					}
				}
			}
			return result, nil
		}

		switch err.(type) {
		case *util.RedisError:
			err.(*util.RedisError).Function = SACN_BUCKET_OBJS
		default:
			return nil, err
		}
		return nil, err
	}

	for {
		objs, err := op(&cursor)
		if err != nil || cursor == "0" {
			close(ch)
			break
		}
		//objects = append(objects, objs...)
		for i := range objs {
			ch <- objs[i]
		}
	}
	return objects, err
}

func (a *ScanRedisHandler) GETObjToFid(obj string) (*FidInfo, error) {
	res, err := a.redisBaseHandler.GET(obj)
	if err != nil {
		return nil, &util.RedisError{
			Message:  "Switch get fid failed.",
			Function: "GetObjToFid",
		}
	}

	var objInfo js.ObjectInfo
	if err = json.Unmarshal([]byte(res), &objInfo); err != nil {
		return nil, err
	}

	switch objInfo.IndexType {
	case 1:
		blocks := objInfo.Blocks
		fid := make([]string, len(blocks))
		for i := 0; i < len(blocks); i++ {
			fid = append(fid, blocks[i].Key)
		}

		return &FidInfo{
			bucket: objInfo.BucketName,
			object: objInfo.ObjectName,
			index:  objInfo.IndexType,
			fids:   fid,
		}, nil
	case 2:
		//todo: do something
	case 3:
		hashkey := "OBJECTPART_GROUP#" + objInfo.BucketName + "#" + objInfo.ObjectName + "#" + objInfo.UploadID
		fidInfo, err := a.HGETALLFidsFromPartGp(hashkey)
		if err != nil {
			return nil, err
		}
		return &FidInfo{
			bucket: objInfo.BucketName,
			object: objInfo.ObjectName,
			index:  objInfo.IndexType,
			fids:   fidInfo,
		}, nil
	}

	return nil, err
}

//todo: parse index 3 obj to fids
func (a *ScanRedisHandler) HGETALLFidsFromPartGp(partKey string) ([]string, error) {
	var (
		partNumber   int    = 0
		err          error  = nil
		cursor       string = "0"
		completeFids []string
	)
	op := func(cursor *string) (int, error) {
		var res []interface{}
		res, err = a.redisBaseHandler.HSCAN(partKey, *cursor, "part_*", "200")
		if err == nil {
			if len(res) != 2 {
				return 0, &util.RedisError{}
			}
			// update cursor
			*cursor = string(res[0].([]byte))
			if res[1] == nil {
				return 0, nil
			}
			resVal := res[1].([]interface{})
			if len(resVal)%2 != 0 {
				return 0, &util.RedisError{Message: "replay elements %2 != 0", Function: "HSCANPartNumber"}
			}
			return len(resVal) / 2, nil
		}
		switch err.(type) {
		case *util.RedisError:
			err.(*util.RedisError).Function = "hscan_partmember_number"
		default:
			return 0, err
		}
		return 0, err
	}

	for {
		p, err := op(&cursor)
		partNumber += int(p)
		if err != nil || cursor == "0" {
			break
		}
	}

	makeKey := func(key string) string {
		tmpStrs := strings.Split(key, "#")
		tmpStrs[0] = "OBJECTPART"
		tmpStr := strings.Join(tmpStrs, "#")
		return tmpStr
	}

	key := makeKey(partKey)
	for i := 1; i <= partNumber; i++ {
		newKey := key + "#" + strconv.Itoa(i)
		fidss, x := a.hgetallObjPart(newKey)
		if x != nil {
			return nil, x
		}
		completeFids = append(completeFids, fidss...)
	}
	return completeFids, nil

}

func (a *ScanRedisHandler) hgetallObjPart(key string) ([]string, error) {
	var (
		keyAndLen struct {
			Key string `json:"key"`
			Len int    `json:"len"`
		}
		tmpStr string
		blocks []string
		err    error
	)
	res, err := a.redisBaseHandler.HGETALL(key)
	if err == nil {
		retVal := res.([]interface{})
		if len(retVal)%2 != 0 {
			return nil, &util.RedisError{Message: "reply elements %2 != 0",
				Function: "HGETALLBlockKeysFromObjectPart"}
		}
		for i := 0; i < len(retVal); i++ {
			i++
			switch retVal[i].(type) {
			case string:
				tmpStr = retVal[i].(string)
			case []byte:
				tmpStr = string(retVal[i].([]byte))
			default:
				return nil, &util.RedisError{
					Message:  "Hgetall block keys from object part result failed.",
					Function: "hgetallObjPart",
				}
			}
			if err = json.Unmarshal([]byte(tmpStr), &keyAndLen); err != nil {
				return blocks, &util.RedisError{Message: "parse json failed",
					Function: "HGETALLBlockKeysFromObjectPart", ErrorInfo: err.Error()}
			}
			blocks = append(blocks, keyAndLen.Key)
		}
		return blocks, nil
	}
	switch err.(type) {
	case *util.RedisError:
		err.(*util.RedisError).Function = "hgetallObjPart"
	default:
		return nil, err
	}
	return nil, err
}
