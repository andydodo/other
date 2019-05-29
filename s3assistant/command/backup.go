package command

import (
	"fmt"
	"net/http"
	"os"
	"s3assistant/backup"
	"s3assistant/common"
	"s3assistant/util"
	"time"
)

var (
	b BackupOptions
)

var cmdBackup = &Command{
	UsageLine: "backup -config=./config.yaml",
	Short:     "incremental backup all objects from source bucket to destination bucket",
	Long:      "",
}

type BackupOptions struct {
	configFile *string
}

func init() {
	b.configFile = cmdBackup.Flag.String("config", "", "Backup config file path")
	cmdBackup.Run = runBackup
}

func runBackup(stop chan struct{}) bool {
	// 1. create handler entry
	handlerEntry, err := b.CreateCommandEntry()
	if err != nil {
		fmt.Fprintf(os.Stderr, "create command entry failed %s", err.Error())
		return false
	}
	// 2. Initialize and run
	if !handlerEntry.Init() {
		fmt.Fprintf(os.Stderr, "init command entry failed")
		return false
	}
	cmdBackup.Status = CommandInit
	handlerEntry.Start()
	cmdBackup.Status = CommandProcessing

	// 3. Start goroutine to handle signal
	go func() {
		defer func() {
			cmdBackup.Status = CommandExited
		}()
		for {
			select {
			case <-stop:
				handlerEntry.Exit()
				return
			default:
				time.Sleep(time.Second * DefaultSleepTime)
			}
		}
	}()
	return true
}

func (b *BackupOptions) CreateCommandEntry() (common.HandlerEntry, error) {
	var backupConf backup.BackupConf
	if ok, err := util.ParseConf(*b.configFile, &backupConf); !ok {
		return nil, err
	}
	if backupConf.RedisPoolConfBase == nil {
		return nil, &util.RedisError{
			Message:  "BackupConf.RedisPoolConfBase is nil",
			Function: "nil",
		}
	}
	redisConnectionNumber := backupConf.WorkerNums + 10 + backupConf.QueueShardingNumber*2
	var redisPoolConf *common.RedisPoolConf = common.NewRedisPoolConf(
		backupConf.RedisPoolConfBase,
		redisConnectionNumber,
		redisConnectionNumber)
	var redisBaseHandler *common.RedisBaseHandler = common.NewRedisBaseHandler(common.NewRedisPool(redisPoolConf))
	if err := redisBaseHandler.ValidateRedisConnection(); err != nil {
		return nil, &util.RedisError{
			Message:  "validate redis connection failed",
			Function: "ping",
		}
	}

	var httpClient *http.Client = common.NewHttpClient(
		backupConf.WorkerNums,
		backupConf.HttpKeepAlivePeriod,
		backupConf.HttpIdleConnTimeout)

	logger, err := common.LoggerWrapper.CreateLogger(backupConf.LogDir, "backup", backupConf.LogLevel)
	if err != nil {
		return nil, err
	}
	backupHandler := backup.NewBackupHandler(logger,
		httpClient,
		redisBaseHandler,
		&backupConf)
	return backupHandler, nil
}
