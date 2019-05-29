package backup

import (
	"net/http"
	"time"

	"s3assistant/common"
	"s3assistant/s3entry"

	"go.uber.org/zap"
)

// Backup item processing status
const (
	COMPLETED_TIMESTAMP_STRING = "COMPLETED" // used to mark backup item have been completed, set by worker, check by checker
	WAITING_TIMESTAMP_STRING   = "WAITING"   // used to mark backup item have been added to worker, set by dispacther, check bu checker
)

const (
	PARSE_BACKUP_ITEM_INVAILD_INT64_RETURN = -10
	WAIT_SLEEP_SECONDS                     = 1 // sleep time waiting for next operation
	CHANNEL_BUFFER_SIZE                    = 1024
)

const (
	DELETE_OPERATION = "delete"
	PUT_OPERATION    = "put"
)

type BackupConf struct {
	RedisPoolConfBase *common.RedisPoolConfBase `yaml:"redis_pool_conf"`

	DestinationBucket         string          `yaml:"destination_bucket"` // the destination bucket
	TempFileDir               string          `yaml:"temp_file_dir"`      // directory for store temp file in lcoal
	StartDispatcher           bool            `yaml:"start_dispatcher"`   // used to open/close consume srcQueue
	QueuePrefix               string          `yaml:"queue_prefix"`
	QueueShardingNumber       int             `yaml:"queue_sharding_num"`
	MinistPartSizeBytes       uint64          `yaml:"minist_part_size_bytes"`  // the minist part size used by multipart upload
	KeepAliveSleepTimeSeconds int64           `yaml:"keep_alive_sleep_time_s"` // the period of update time for worker to keep alive
	SrcS3Conf                 *s3entry.S3Conf `yaml:"src_s3_conf"`
	DstS3Conf                 *s3entry.S3Conf `yaml:"dst_s3_conf"`

	HttpIdleConnTimeout int64  `yaml:"http_idle_conn_timeout_s"`
	HttpKeepAlivePeriod int64  `yaml:"http_keep_alive_period_s"`
	WorkerNums          int    `yaml:"worker_numbers"`
	LogDir              string `yaml:"log_dir"`
	LogLevel            int    `yaml:"log_level"`
}

type BackupHandler struct {
	// the following fields represent the three components:
	// worker, dispacther and checker
	workerManager     *WorkerManager
	dispatcherManager *DispatcherManager
	checkerManager    *CheckerManager

	handlerStatus                  common.HandlerStatus
	logger                         *zap.Logger
	backupConf                     *BackupConf
	redisHandler                   *BackupRedisHandler
	httpClient                     *http.Client
	workerNumber, controllerNumber int

	// the following fields used to communicate within components
	// workerAndQueue channel used for worker to fetch item from
	workerAndQueue []chan *BackupChannelInfo
	// checkerRedeliverChannel used for checker redeliver failed item to dispacther
	checkerRedeliverChannel []chan *BackupChannelInfo
	// workerRedeliverChannel used for worker redeliver failed item to dispacther
	workerRedeliverChannel []chan *BackupChannelInfo
	// continueCheckerChannel used for checker to decide when to start check, controlled by checker and dispacther
	continueCheckerChannel []chan struct{}
}

func NewBackupHandler(logger *zap.Logger, httpClient *http.Client,
	redisBaseHandler *common.RedisBaseHandler, backupConf *BackupConf) *BackupHandler {
	if backupConf.QueueShardingNumber <= 0 {
		backupConf.QueueShardingNumber = 1
	}
	return &BackupHandler{
		logger:           logger,
		backupConf:       backupConf,
		redisHandler:     NewBackupRedisHandler(logger, redisBaseHandler),
		handlerStatus:    common.HANDLER_NONE,
		httpClient:       httpClient,
		workerNumber:     backupConf.WorkerNums,
		controllerNumber: backupConf.QueueShardingNumber,
	}
}

func (b *BackupHandler) Init() bool {
	b.workerAndQueue = make([]chan *BackupChannelInfo, b.workerNumber)
	for i := 0; i < b.workerNumber; i++ {
		b.workerAndQueue[i] = make(chan *BackupChannelInfo, CHANNEL_BUFFER_SIZE)
	}
	b.checkerRedeliverChannel = make([]chan *BackupChannelInfo, b.controllerNumber)
	b.workerRedeliverChannel = make([]chan *BackupChannelInfo, b.controllerNumber)
	b.continueCheckerChannel = make([]chan struct{}, b.controllerNumber)
	for i := 0; i < b.controllerNumber; i++ {
		b.workerRedeliverChannel[i] = make(chan *BackupChannelInfo, CHANNEL_BUFFER_SIZE)
		b.checkerRedeliverChannel[i] = make(chan *BackupChannelInfo, CHANNEL_BUFFER_SIZE)
		b.continueCheckerChannel[i] = make(chan struct{}, 1)
	}
	// create component
	b.workerManager = NewWorkerManager(b.logger,
		b.backupConf,
		b.redisHandler,
		b.httpClient,
		b.workerAndQueue,
		b.workerRedeliverChannel)
	b.dispatcherManager = NewDispatcherManager(b.logger,
		b.backupConf,
		b.redisHandler,
		b.workerAndQueue,
		b.checkerRedeliverChannel,
		b.workerRedeliverChannel,
		b.continueCheckerChannel)
	b.checkerManager = NewCheckerManager(b.logger,
		b.backupConf,
		b.redisHandler,
		b.checkerRedeliverChannel,
		b.continueCheckerChannel)
	// init component
	if !b.initComponent() {
		return false
	}
	b.startRecycleGoroutine()
	b.handlerStatus = common.HANDLER_INITIAL
	return true
}

func (b *BackupHandler) initComponent() (res bool) {
	return b.workerManager.Init() && b.checkerManager.Init() && b.dispatcherManager.Init()
}

func (b *BackupHandler) startRecycleGoroutine() {
	// Start goroutine to recycle goroutine
	go func() {
		defer func() {
			b.handlerStatus = common.HANDLER_EXITED
			b.logger.Error("backup handler exit successfully.")
		}()
		for {
			select {
			case <-b.workerManager.WorkerManagerExitedChannel:
				b.Exit()
				return
			case <-b.dispatcherManager.dispatcherManagerExitedChannel:
				b.Exit()
				return
			case <-b.checkerManager.checkerManagerExitedChannel:
				b.Exit()
				return
			default:
				time.Sleep(time.Second * WAIT_SLEEP_SECONDS)
				break
			}
		}
	}()
}

func (b *BackupHandler) Exit() {
	// stop dispatcher
	b.dispatcherManager.Exit()
	// stop checker
	b.checkerManager.Exit()
	// stop worker
	b.workerManager.Exit()
}

func (b *BackupHandler) Status() common.HandlerStatus {
	return b.handlerStatus
}

func (b *BackupHandler) Start() {
	// start wroker goroutine to run backup
	b.workerManager.StartWorker()
	// start dispatcher goroutine to fetch backup item
	b.dispatcherManager.StartDispatcher()
	// start check goroutine to check if backup item have been completed
	b.checkerManager.StartChecker()
}
