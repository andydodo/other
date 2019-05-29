package command

import (
	"fmt"
	"net/http"
	"os"
	"s3assistant/asyncdelete"
	"s3assistant/common"
	"s3assistant/util"
	"time"
)

var (
	a AsyncDeleteOptions
)

var cmdAsyncDelete = &Command{
	UsageLine: "asyncdelete -config=./config.yaml",
	Short:     "delete the objects marked in s3-gateway",
	Long:      "",
}

type AsyncDeleteOptions struct {
	configFile *string
}

func init() {
	a.configFile = cmdAsyncDelete.Flag.String("config", "", "AsyncDeleteOptions config file path")
	cmdAsyncDelete.Run = runAsyncDelete
}

func runAsyncDelete(stop chan struct{}) bool {
	// 1. create handler entry
	handlerEntry, err := a.CreateCommandEntry()
	if err != nil {
		fmt.Fprintf(os.Stderr, "create command entry failed %s", err.Error())
		return false
	}
	// 2. Initialize and run
	if !handlerEntry.Init() {
		fmt.Fprintf(os.Stderr, "init command entry failed")
		return false
	}
	cmdAsyncDelete.Status = CommandInit
	handlerEntry.Start()
	cmdAsyncDelete.Status = CommandProcessing

	// 3. Start goroutine to handle signal
	go func() {
		defer func() {
			cmdAsyncDelete.Status = CommandExited
		}()
		for {
			select {
			case <-stop:
				handlerEntry.Exit()
				return
			default:
				if handlerEntry.Status() == common.HANDLER_EXITED {
					return
				}
				time.Sleep(time.Second * DefaultSleepTime)
			}
		}
	}()
	return true
}

func (a *AsyncDeleteOptions) CreateCommandEntry() (common.HandlerEntry, error) {
	var asyncDeleteConf asyncdelete.AsyncDeleteConf
	if ok, err := util.ParseConf(*a.configFile, &asyncDeleteConf); !ok {
		return nil, err
	}
	if asyncDeleteConf.RedisPoolConfBase == nil {
		return nil, &util.RedisError{
			Message:  "BackupConf.RedisPoolConfBase is nil",
			Function: "nil",
		}
	}
	redisConnectionNumber := asyncDeleteConf.WorkerNums + 10
	var redisPoolConf *common.RedisPoolConf = common.NewRedisPoolConf(asyncDeleteConf.RedisPoolConfBase,
		redisConnectionNumber, redisConnectionNumber)
	var redisBaseHandler *common.RedisBaseHandler = common.NewRedisBaseHandler(common.NewRedisPool(redisPoolConf))
	if err := redisBaseHandler.ValidateRedisConnection(); err != nil {
		return nil, &util.RedisError{
			Message:  "validate redis connection failed",
			Function: "ping",
		}
	}

	var httpClient *http.Client = common.NewHttpClient(
		asyncDeleteConf.WorkerNums,
		asyncDeleteConf.HttpKeepAlivePeriod,
		asyncDeleteConf.HttpIdleConnTimeout)

	logger, err := common.LoggerWrapper.CreateLogger(asyncDeleteConf.LogDir, "asyncdelete", asyncDeleteConf.LogLevel)
	if err != nil {
		return nil, err
	}
	asyncDeleteHandler := asyncdelete.NewAsyncDeleteHandler(logger,
		httpClient,
		redisBaseHandler,
		&asyncDeleteConf)
	return asyncDeleteHandler, nil
}
