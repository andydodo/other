package asyncdelete

import (
	"fmt"
	"net"
	"net/http"
	"time"

	"s3assistant/common"

	"go.uber.org/zap"
)

const (
	IDLE_SLEEP_SECONDS = 5
)

type AsyncDeleteConf struct {
	RedisPoolConfBase     *common.RedisPoolConfBase `yaml:"redis_pool_conf"`
	BlockReservedTime     int64                     `yaml:"block_reserved_time_s"`
	DeletedBlockList      string                    `yaml:"deleted_block_list"`
	MasterAddrs           []string                  `yaml:"master_addrs"`
	TempBlockSetKeyPrefix string                    `yaml:"temp_block_set_key_prefix"`

	HttpIdleConnTimeout int64  `yaml:"http_idle_conn_timeout_s"`
	HttpKeepAlivePeriod int64  `yaml:"http_keep_alive_period_s"`
	WorkerNums          int    `yaml:"worker_numbers"`
	LogDir              string `yaml:"log_dir"`
	LogLevel            int    `yaml:"log_level"`
}

type AsyncDeleteHandler struct {
	workerManager *common.WorkerManager

	logger        *zap.Logger
	redisHandler  *AsyncDeleteRedisHandler
	swClient      *SwClient
	handlerStatus common.HandlerStatus
}

func NewAsyncDeleteHandler(logger *zap.Logger, httpClient *http.Client,
	redisBaseHandler *common.RedisBaseHandler, asyncDeleteConf *AsyncDeleteConf) *AsyncDeleteHandler {
	return &AsyncDeleteHandler{
		logger:        logger,
		workerManager: common.NewWorkerManager(logger, asyncDeleteConf.WorkerNums),
		redisHandler: NewAsyncDeleteRedisHandler(logger, redisBaseHandler,
			asyncDeleteConf.BlockReservedTime, asyncDeleteConf.DeletedBlockList, asyncDeleteConf.TempBlockSetKeyPrefix),
		swClient: NewSwClient(logger, httpClient, asyncDeleteConf.MasterAddrs),
	}
}

func (a *AsyncDeleteHandler) Init() bool {
	a.workerManager.Init()
	a.handlerStatus = common.HANDLER_INITIAL
	go func() {
		defer func() {
			a.handlerStatus = common.HANDLER_EXITED
			a.logger.Error("AsyncDeleteHandler exit successfully")
		}()
		for {
			select {
			case <-a.workerManager.WorkerManagerExitedChannel:
				a.Exit()
				return
			default:
				time.Sleep(time.Second * IDLE_SLEEP_SECONDS)
			}
		}
	}()
	return true
}

func (a *AsyncDeleteHandler) Exit() {
	a.workerManager.Exit()
}

func (a *AsyncDeleteHandler) Status() common.HandlerStatus {
	return a.handlerStatus
}

func (a *AsyncDeleteHandler) Start() {
	for i := 0; i < a.workerManager.WorkerNums; i++ {
		go func(workerId int) {
			defer func() {
				if err := recover(); err != nil {
					a.logger.Error("AsyncDeleteHandler worker catch a panic",
						zap.Int("WorkerID", workerId),
						zap.String("Panic", fmt.Sprintf("panic recover: %v", err)))
				}
				a.logger.Error("AsyncDeleteHandler worker exit", zap.Int("workerID", workerId))
				a.logger.Sync()
				a.workerManager.WorkerExitedChannel <- struct{}{}
				a.workerManager.WorkerWaitGroup.Done()
			}()
			waitSleepTimeCoefficient := 1
			logIdleCount := 0
			for {
				select {
				case <-a.workerManager.StopWorkerChannel:
					return
				default:
					// 1. Get block key
					blockKey, continueFlag := a.getBlockKey()
					if !continueFlag {
						a.logger.Error("getBlockKey failed, unrecoverable error occurred")
						return
					}
					if blockKey == NET_ERROR_STRING_RETURN {
						a.logger.Error("getBlockKey net error occurred, sleep for a while")
						time.Sleep(time.Second * time.Duration(waitSleepTimeCoefficient*NET_ERROR_SLEEP_SECONDS))
						waitSleepTimeCoefficient += 1
						break
					}
					if blockKey == common.EMPTY_STRING {
						if logIdleCount%LOG_IDLE_THRESHOLD == 0 {
							a.logger.Info("There are no keys need to delete right now")
						}
						if logIdleCount < MAX_IDLE_COUNT {
							logIdleCount += 1
						} else {
							logIdleCount = 0
						}
						time.Sleep(time.Second * IDLE_SLEEP_SECONDS)
						break
					}
					logIdleCount = 0
					a.logger.Debug("get block key successfully", zap.String("blockKey", blockKey))
					// 2. Check if exist block reference
					getResp, continueFlag := a.redisHandler.getBlockRefWithRetry(blockKey)
					if !continueFlag {
						a.logger.Error("getBlockRefWithRetry failed, unrecoverable error occurred",
							zap.String("blockKey", blockKey))
						return
					}
					if getResp == NET_ERROR_INT64_RETURN {
						a.logger.Error("getBlockRefWithRetry net error occurred, sleep for a while",
							zap.String("blockKey", blockKey))
						time.Sleep(time.Second * time.Duration(waitSleepTimeCoefficient*NET_ERROR_SLEEP_SECONDS))
						waitSleepTimeCoefficient += 1
						break
					}
					// 3. DecreaseRef
					var newNum int64 = -1
					if getResp != NOT_FOUND_INT64_RETURN {
						newNum, continueFlag = a.redisHandler.IncreaseRef(blockKey, -1)
						if !continueFlag {
							a.logger.Error("Increase reference failed, unrecoverable error occurred",
								zap.String("blockKey", blockKey))
							return
						}
						if newNum == NET_ERROR_INT64_RETURN {
							a.logger.Error("Increase reference net error occurred, sleep for a while",
								zap.String("blockKey", blockKey))
							time.Sleep(time.Second * time.Duration(waitSleepTimeCoefficient*NET_ERROR_SLEEP_SECONDS))
							waitSleepTimeCoefficient += 1
							break
						}
						a.logger.Debug("Increase reference successfully",
							zap.Int64("newReferenceNumber", newNum),
							zap.String("blockKey", blockKey))
						// del block reference when reference count less than zero
						if newNum < 0 {
							delResp, continueFlag := a.redisHandler.delRefWithRetry(blockKey)
							if !continueFlag {
								a.logger.Error("delFileWithRetry failed, unrecoverable error occurred",
									zap.String("blockKey", blockKey))
								return
							}
							if delResp == NET_ERROR_INT64_RETURN {
								a.logger.Error("delRefWithRetry net error occurred, sleep for a while",
									zap.String("blockKey", blockKey))
								time.Sleep(time.Second * time.Duration(waitSleepTimeCoefficient*NET_ERROR_SLEEP_SECONDS))
								waitSleepTimeCoefficient += 1
								break
							}
							a.logger.Info("Delete reference successfully",
								zap.String("blockKey", blockKey))
						}
					}
					// 4.Del file when block reference less than zero
					if newNum < 0 {
						// 4.1. request volume url
						volumeUrl, continueFlag := a.swClient.requestVolumeURLWithRetry(blockKey)
						if !continueFlag {
							a.logger.Error("requestVolumeURLWithRetry failed, unrecoverable error occurred",
								zap.String("blockKey", blockKey))
							return
						}
						if volumeUrl == NET_ERROR_STRING_RETURN {
							a.logger.Error("requestVolumeURLWithRetry net error occurred, sleep for a while",
								zap.String("blockKey", blockKey))
							time.Sleep(time.Second * time.Duration(waitSleepTimeCoefficient*NET_ERROR_SLEEP_SECONDS))
							waitSleepTimeCoefficient += 1
							break
						}
						if volumeUrl == common.EMPTY_STRING {
							a.logger.Info("volume not found", zap.String("blockKey", blockKey))
							break
						}
						a.logger.Info("request volume URL successfully",
							zap.String("blockKey", blockKey),
							zap.String("volumeURL", volumeUrl))
						// 4.2. del file
						operateSuccess, continueFlag := a.swClient.delFileWithRetry(blockKey, volumeUrl)
						if !continueFlag {
							a.logger.Error("delFileWithRetry failed, unrecoverable error occurred",
								zap.String("blockKey", blockKey))
							return
						}
						if !operateSuccess {
							a.logger.Error("delFileWithRetry net/http error occurred, sleep for a while",
								zap.String("blockKey", blockKey))
							time.Sleep(time.Second * time.Duration(waitSleepTimeCoefficient*NET_ERROR_SLEEP_SECONDS))
							waitSleepTimeCoefficient += 1
							break
						}
						a.logger.Info("Delete file successfully", zap.String("blockKey", blockKey))
					}
					waitSleepTimeCoefficient = 1
				}
				a.logger.Sync()
			}
		}(i)
	}
}

// getBlockKey fetch the expired blockKey, return the key and
// the boolean indicated if unrecoverable error occurred or not.
func (a *AsyncDeleteHandler) getBlockKey() (string, bool) {
	blockKey, err := a.redisHandler.GetDeletedBlockKey()
	if err == nil {
		return blockKey, true
	}
	a.logger.Error("getBlockKey failed", zap.String("error", err.Error()))
	switch err.(type) {
	case net.Error:
		return NET_ERROR_STRING_RETURN, true
	default:
		return common.EMPTY_STRING, false
	}
}
