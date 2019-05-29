package common

import (
	"fmt"
	"time"

	"s3assistant/util"

	"github.com/gomodule/redigo/redis"
)

const (
	REDIS_REPLY_INVALID_INT64_RETURN = -100
	EMPTY_STRING                     = ""
)

// redisPoolConfBase configed by user
type RedisPoolConfBase struct {
	Host        string `yaml:"redis_ip"`
	Port        int    `yaml:"redis_port"`
	Password    string `yaml:"redis_passwd"`
	Db          int    `yaml:"redis_db"`
	Timeout     int    `yaml:"redis_timeout_s"`
	IdleTimeout int    `yaml:"redis_idle_timeout_s"`
}

type RedisPoolConf struct {
	redisPoolConfBase *RedisPoolConfBase
	maxIdleConn       int
	maxActiveConn     int
}

func NewRedisPoolConf(redisPoolConfBase *RedisPoolConfBase,
	maxIdleConn int,
	maxActiveConn int) *RedisPoolConf {
	return &RedisPoolConf{
		redisPoolConfBase: redisPoolConfBase,
		maxIdleConn:       maxIdleConn,
		maxActiveConn:     maxActiveConn,
	}
}

func NewRedisPool(redisPoolConf *RedisPoolConf) *redis.Pool {
	address := fmt.Sprintf("%v:%v", redisPoolConf.redisPoolConfBase.Host, redisPoolConf.redisPoolConfBase.Port)
	passwd := redis.DialPassword(redisPoolConf.redisPoolConfBase.Password)
	db := redis.DialDatabase(redisPoolConf.redisPoolConfBase.Db)
	readTimeout := redis.DialReadTimeout(time.Second * time.Duration(redisPoolConf.redisPoolConfBase.Timeout))
	writeTimeout := redis.DialWriteTimeout(time.Second * time.Duration(redisPoolConf.redisPoolConfBase.Timeout))
	conTimeout := redis.DialConnectTimeout(time.Second * time.Duration(redisPoolConf.redisPoolConfBase.Timeout))
	return &redis.Pool{
		MaxIdle:     redisPoolConf.maxIdleConn,
		MaxActive:   redisPoolConf.maxActiveConn,
		Wait:        true,
		IdleTimeout: time.Duration(redisPoolConf.redisPoolConfBase.IdleTimeout) * time.Second,

		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", address, db, passwd, readTimeout,
				writeTimeout, conTimeout)
			if err != nil {
				return nil, err
			}
			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}
}

type RedisBaseHandler struct {
	*redis.Pool
}

func NewRedisBaseHandler(redisPool *redis.Pool) *RedisBaseHandler {
	return &RedisBaseHandler{
		Pool: redisPool,
	}
}

func (r *RedisBaseHandler) ValidateRedisConnection() error {
	connect := r.Get()
	defer connect.Close()
	_, err := connect.Do("PING")
	return err
}

func (r *RedisBaseHandler) SPOP(key string) (string, error) {
	connect := r.Get()
	defer connect.Close()
	res, err := connect.Do("SPOP", key)
	if err != nil {
		return EMPTY_STRING, err
	}
	switch res.(type) {
	case nil:
		return EMPTY_STRING, nil
	case error:
		return EMPTY_STRING, &util.RedisError{
			Message:   "SPOP reply error",
			ErrorInfo: res.(error).Error(),
		}
	case []byte:
		return string(res.([]byte)), nil
	}
	return EMPTY_STRING, &util.RedisError{
		Message: "SPOP unkown error",
	}
}

func (r *RedisBaseHandler) SADD(key string, value ...string) (int64, error) {
	connect := r.Get()
	defer connect.Close()
	var args []interface{}
	args = append(args, key)
	for _, arg := range value {
		args = append(args, arg)
	}
	res, err := connect.Do("SADD", args...)
	if err != nil {
		return REDIS_REPLY_INVALID_INT64_RETURN, err
	}
	switch res.(type) {
	case error:
		return REDIS_REPLY_INVALID_INT64_RETURN, &util.RedisError{
			Message:   "SADD reply error",
			ErrorInfo: res.(error).Error(),
		}
	case int64:
		return res.(int64), nil
	}
	return REDIS_REPLY_INVALID_INT64_RETURN, &util.RedisError{
		Message: "SADD unkown error",
	}
}

func (r *RedisBaseHandler) SDIFFSTORE(destinationKey string, sourceKeys ...string) (int64, error) {
	connect := r.Get()
	defer connect.Close()
	var args []interface{}
	args = append(args, destinationKey)
	for _, arg := range sourceKeys {
		args = append(args, arg)
	}
	res, err := connect.Do("SDIFFSTORE", args...)
	if err != nil {
		return REDIS_REPLY_INVALID_INT64_RETURN, err
	}
	switch res.(type) {
	case error:
		return REDIS_REPLY_INVALID_INT64_RETURN, &util.RedisError{
			Message:   "SDIFFSTORE reply error",
			ErrorInfo: res.(error).Error(),
		}
	case int64:
		return res.(int64), nil
	}
	return REDIS_REPLY_INVALID_INT64_RETURN, &util.RedisError{
		Message: "SDIFFSTORE unkown error",
	}
}

func (r *RedisBaseHandler) SCAN(cursor, prefix, count string) ([]interface{}, error) {
	connect := r.Get()
	defer connect.Close()
	res, err := connect.Do("SCAN", cursor, "MATCH", prefix, "COUNT", count)
	if err != nil {
		return nil, err
	}
	switch res.(type) {
	case error:
		return nil, &util.RedisError{
			Message:   "Scan reply error",
			ErrorInfo: res.(error).Error(),
		}
	case []interface{}:
		return res.([]interface{}), nil
	}
	return nil, &util.RedisError{
		Message: "Scan reply error",
	}
}

func (r *RedisBaseHandler) SSCAN(key, cursor, prefix, count string) ([]interface{}, error) {
	connect := r.Get()
	defer connect.Close()
	res, err := connect.Do("SSCAN", key, cursor, "MATCH", prefix, "COUNT", count)
	if err != nil {
		return nil, err
	}
	switch res.(type) {
	case error:
		return nil, &util.RedisError{
			Message:   "Sscan reply error",
			ErrorInfo: res.(error).Error(),
		}
	case []interface{}:
		return res.([]interface{}), nil
	}
	return nil, &util.RedisError{
		Message: "Sscan reply error",
	}
}

func (r *RedisBaseHandler) RPOP(key string) (string, error) {
	connect := r.Get()
	defer connect.Close()
	res, err := connect.Do("RPOP", key)
	if err != nil {
		return EMPTY_STRING, err
	}
	switch res.(type) {
	case nil:
		return EMPTY_STRING, nil
	case error:
		return EMPTY_STRING, &util.RedisError{
			Message:   "RPOP reply error",
			ErrorInfo: res.(error).Error(),
		}
	case []byte:
		return string(res.([]byte)), nil
	}
	return EMPTY_STRING, &util.RedisError{
		Message: "RPOP unkown error",
	}
}

func (r *RedisBaseHandler) LPUSH(key string, values ...string) error {
	connect := r.Get()
	defer connect.Close()
	var args []interface{}
	args = append(args, key)
	for _, arg := range values {
		args = append(args, arg)
	}
	fmt.Println(args)
	res, err := connect.Do("LPUSH", args...)
	if err != nil {
		return err
	}
	switch res.(type) {
	case error:
		return &util.RedisError{
			Message:   "LPUSH reply error",
			ErrorInfo: res.(error).Error(),
		}
	case int64:
		return nil
	}
	return &util.RedisError{
		Message: "LPUSH unkown error",
	}
}

func (r *RedisBaseHandler) RPUSH(key, value string) error {
	connect := r.Get()
	defer connect.Close()
	res, err := connect.Do("RPUSH", key, value)
	if err != nil {
		return err
	}
	switch res.(type) {
	case error:
		return &util.RedisError{
			Message:   "RPUSH reply error",
			ErrorInfo: res.(error).Error(),
		}
	case int64:
		return nil
	}
	return &util.RedisError{
		Message: "RPUSH unkown error",
	}
}

func (r *RedisBaseHandler) GET(key string) (string, error) {
	connect := r.Get()
	defer connect.Close()
	res, err := connect.Do("GET", key)
	if err != nil {
		return EMPTY_STRING, err
	}
	switch res.(type) {
	case nil:
		return EMPTY_STRING, nil
	case error:
		return EMPTY_STRING, &util.RedisError{
			Message:   "GET reply error",
			ErrorInfo: res.(error).Error(),
		}
	case []byte:
		return string(res.([]byte)), nil
	}
	return EMPTY_STRING, &util.RedisError{
		Message: "GET unkown error",
	}
}

func (r *RedisBaseHandler) INCRBY(key string, num int64) (int64, error) {
	connect := r.Get()
	defer connect.Close()
	res, err := connect.Do("INCRBY", key, num)
	if err != nil {
		return REDIS_REPLY_INVALID_INT64_RETURN, err
	}
	switch res.(type) {
	case error:
		return REDIS_REPLY_INVALID_INT64_RETURN, &util.RedisError{
			Message:   "INCRBY reply error",
			ErrorInfo: res.(error).Error(),
		}
	case int64:
		return res.(int64), nil
	}
	return REDIS_REPLY_INVALID_INT64_RETURN, &util.RedisError{
		Message: "INCRBY unkown error",
	}
}

func (r *RedisBaseHandler) DEL(key string) (int64, error) {
	connect := r.Get()
	defer connect.Close()
	res, err := connect.Do("DEL", key)
	if err != nil {
		return REDIS_REPLY_INVALID_INT64_RETURN, err
	}
	switch res.(type) {
	case error:
		return REDIS_REPLY_INVALID_INT64_RETURN, &util.RedisError{
			Message:   "DEL reply error",
			ErrorInfo: res.(error).Error(),
		}
	case int64:
		return res.(int64), nil
	}
	return REDIS_REPLY_INVALID_INT64_RETURN, &util.RedisError{
		Message: "DEL unkown error",
	}
}

func (r *RedisBaseHandler) RPOPLPUSH(srcQueue, dstQueue string) (string, error) {
	connect := r.Get()
	defer connect.Close()
	res, err := connect.Do("RPOPLPUSH", srcQueue, dstQueue)
	if err != nil {
		return EMPTY_STRING, err
	}
	switch res.(type) {
	case nil:
		return EMPTY_STRING, nil
	case error:
		return EMPTY_STRING, &util.RedisError{
			Message:   "RPOPLPUSH reply error",
			ErrorInfo: res.(error).Error(),
		}
	case []byte:
		return string(res.([]byte)), nil
	}
	return EMPTY_STRING, &util.RedisError{
		Message: "RPOPLPUSH unkown error",
	}
}

func (r *RedisBaseHandler) SETNX(stringKey, stringValue string) (int64, error) {
	connect := r.Get()
	defer connect.Close()
	res, err := connect.Do("SETNX", stringKey, stringValue)
	if err != nil {
		return REDIS_REPLY_INVALID_INT64_RETURN, err
	}
	switch res.(type) {
	case error:
		return REDIS_REPLY_INVALID_INT64_RETURN, &util.RedisError{
			Message:   "SETNX reply error",
			ErrorInfo: res.(error).Error(),
		}
	case int64:
		return res.(int64), nil
	}
	return REDIS_REPLY_INVALID_INT64_RETURN, &util.RedisError{
		Message: "SETNX unkown error",
	}
}

func (r *RedisBaseHandler) SET(stringKey, stringValue string) (string, error) {
	connect := r.Get()
	defer connect.Close()
	res, err := connect.Do("SET", stringKey, stringValue)
	if err != nil {
		return EMPTY_STRING, err
	}
	switch res.(type) {
	case error:
		return EMPTY_STRING, &util.RedisError{
			Message:   "SET reply error",
			ErrorInfo: res.(error).Error(),
		}
	case string:
		return res.(string), nil
	}
	return EMPTY_STRING, &util.RedisError{
		Message: "SET unkown error",
	}
}

func (r *RedisBaseHandler) DELVX(stringKey, stringValue string) (int64, error) {
	connect := r.Get()
	defer connect.Close()
	res, err := connect.Do("DELVX", stringKey, stringValue)
	if err != nil {
		return REDIS_REPLY_INVALID_INT64_RETURN, err
	}
	switch res.(type) {
	case error:
		return REDIS_REPLY_INVALID_INT64_RETURN, &util.RedisError{
			Message:   "DELVX reply error",
			ErrorInfo: res.(error).Error(),
		}
	case int64:
		return res.(int64), nil
	}
	return REDIS_REPLY_INVALID_INT64_RETURN, &util.RedisError{
		Message: "DELVX unkown error",
	}
}

func (r *RedisBaseHandler) LINDEX(listName string, index int64) (string, error) {
	connect := r.Get()
	defer connect.Close()
	res, err := connect.Do("LINDEX", listName, index)
	if err != nil {
		return EMPTY_STRING, err
	}
	switch res.(type) {
	case nil:
		return EMPTY_STRING, nil
	case error:
		return EMPTY_STRING, &util.RedisError{
			Message:   "LINDEX reply error",
			ErrorInfo: res.(error).Error(),
		}
	case []byte:
		return string(res.([]byte)), nil
	}
	return EMPTY_STRING, &util.RedisError{
		Message: "LINDEX unkown error",
	}
}

func (r *RedisBaseHandler) LRANGE(listName string, start, stop int64) ([]interface{}, error) {
	connect := r.Get()
	defer connect.Close()
	res, err := connect.Do("LRANGE", listName, start, stop)
	if err != nil {
		return nil, err
	}
	switch res.(type) {
	case nil:
		return nil, nil
	case error:
		return nil, &util.RedisError{
			Message:   "LRANGE reply error",
			ErrorInfo: res.(error).Error(),
		}
	case []interface{}:
		return res.([]interface{}), nil
	}
	return nil, &util.RedisError{
		Message: "LRANGE unkown error",
	}
}

func (r *RedisBaseHandler) LLEN(listName string) (int64, error) {
	connect := r.Get()
	defer connect.Close()
	res, err := connect.Do("LLEN", listName)
	if err != nil {
		return REDIS_REPLY_INVALID_INT64_RETURN, err
	}
	switch res.(type) {
	case error:
		return REDIS_REPLY_INVALID_INT64_RETURN, &util.RedisError{
			Message:   "LLEN reply error",
			ErrorInfo: res.(error).Error(),
		}
	case int64:
		return res.(int64), nil
	}
	return REDIS_REPLY_INVALID_INT64_RETURN, &util.RedisError{
		Message: "LLEN unkown error",
	}
}

func (r *RedisBaseHandler) HGET(key, field string) (string, error) {
	connect := r.Get()
	defer connect.Close()
	res, err := connect.Do("HGET", key, field)
	if err != nil {
		return EMPTY_STRING, err
	}
	switch res.(type) {
	case nil:
		return EMPTY_STRING, nil
	case error:
		return EMPTY_STRING, &util.RedisError{
			Message:   "HGET reply error",
			ErrorInfo: res.(error).Error(),
		}
	case []byte:
		return string(res.([]byte)), nil
	}
	return EMPTY_STRING, &util.RedisError{
		Message: "HGET unkown error",
	}
}

func (r *RedisBaseHandler) HSET(key, field, value string) (int64, error) {
	connect := r.Get()
	defer connect.Close()
	res, err := connect.Do("HSET", key, field, value)
	if err != nil {
		return REDIS_REPLY_INVALID_INT64_RETURN, err
	}
	switch res.(type) {
	case error:
		return REDIS_REPLY_INVALID_INT64_RETURN, &util.RedisError{
			Message:   "HSET reply error",
			ErrorInfo: res.(error).Error(),
		}
	case int64:
		return res.(int64), nil
	}
	return REDIS_REPLY_INVALID_INT64_RETURN, &util.RedisError{
		Message: "HSET unkown error",
	}
}

// HDEL del multi fields require redis verion >= 2.4
func (r *RedisBaseHandler) HDEL(key string, field ...string) (int64, error) {
	connect := r.Get()
	defer connect.Close()
	var args []interface{}
	args = append(args, key)
	for _, arg := range field {
		args = append(args, arg)
	}
	res, err := connect.Do("HDEL", args...)
	if err != nil {
		return REDIS_REPLY_INVALID_INT64_RETURN, err
	}
	switch res.(type) {
	case error:
		return REDIS_REPLY_INVALID_INT64_RETURN, &util.RedisError{
			Message:   "HDEL reply error",
			ErrorInfo: res.(error).Error(),
		}
	case int64:
		return res.(int64), nil
	}
	return REDIS_REPLY_INVALID_INT64_RETURN, &util.RedisError{
		Message: "HDEL unkown error",
	}
}
