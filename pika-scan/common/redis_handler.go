package common

import (
	"fmt"
	"time"

	"github.com/andy/pika-scan/util"
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

func (r *RedisBaseHandler) SADD(key string, value ...int64) (int64, error) {
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
		return REDIS_REPLY_INVALID_INT64_RETURN, &util.RedisError{
			Message:   "SDIFFSTORE reply error",
			Function:  "SDIFFSTORE",
			ErrorInfo: err.Error(),
		}
	}
	switch res.(type) {
	case error:
		return REDIS_REPLY_INVALID_INT64_RETURN, &util.RedisError{
			Message:   "SDIFFSTORE reply error",
			Function:  "SDIFFSTORE",
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

func (r *RedisBaseHandler) HSCAN(key, cursor, prefix, count string) ([]interface{}, error) {
	connect := r.Get()
	defer connect.Close()
	res, err := connect.Do("HSCAN", key, cursor, "MATCH", prefix, "COUNT", count)
	if err != nil {
		return nil, err
	}
	switch res.(type) {
	case error:
		return nil, &util.RedisError{
			Message:   "Hscan reply error",
			ErrorInfo: res.(error).Error(),
		}
	case []interface{}:
		return res.([]interface{}), nil
	}
	return nil, &util.RedisError{
		Message: "Hscan reply error",
	}
}

func (r *RedisBaseHandler) HGETALL(hashKey string) (interface{}, error) {
	connect := r.Get()
	defer connect.Close()
	res, err := connect.Do("HGETALL", hashKey)
	if err != nil {
		return nil, err
	}
	switch res.(type) {
	case error:
		return nil, &util.RedisError{
			Message:   "Hgetall reply error",
			ErrorInfo: res.(error).Error(),
		}
	case interface{}:
		return res.(interface{}), nil
	}
	return nil, &util.RedisError{
		Message: "Hgetall reply error",
	}
}

func (r *RedisBaseHandler) HGETALLV2(hashKey string) ([]interface{}, error) {
	connect := r.Get()
	defer connect.Close()
	res, err := connect.Do("HGETALL", hashKey)
	if err != nil {
		return nil, err
	}
	switch res.(type) {
	case error:
		return nil, &util.RedisError{
			Message:   "Hgetall reply error",
			ErrorInfo: res.(error).Error(),
		}
	case []interface{}:
		return res.([]interface{}), nil
	}
	return nil, &util.RedisError{
		Message: "Hgetall reply error",
	}
}
