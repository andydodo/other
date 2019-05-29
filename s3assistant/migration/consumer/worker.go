package consumer

import (
	"fmt"
	"net/http"
	"s3assistant/migration"
	"s3assistant/migration/broker"
	"s3assistant/s3entry"
	"sync"
	"time"

	"go.uber.org/zap"
)

type WorkerStatus int

const (
	WORKER_INITIAL WorkerStatus = iota
	WORKER_PROCESSING
	WORKER_EXITED
)

type WorkerConf struct {
	SrcS3Conf                 *s3entry.S3Conf    `yaml:"src_s3_conf"`
	DstS3Conf                 *s3entry.S3Conf    `yaml:"dst_s3_conf"`
	BrokerConf                *broker.BrokerConf `yaml:"broker"`
	Queue                     string             `yaml:"queue"`
	ServerPort                string             `yaml:"server_port"`
	TempDir                   string             `yaml:"temp_dir"`  // temp file dir
	LogDir                    string             `yaml:"log_dir"`   // log file dir
	LogLevel                  int                `yaml:"log_level"` // log level
	WorkerNums                int                `yaml:"worker_numbers"`
	MultiUploadThresholdBytes uint64             `yaml:"multi_upload_threshold_bytes"`
}

type Worker struct {
	workerManager *WorkerManager
	s3Handler     *S3Handler
	broker        broker.Broker
	id            int
	queue         string
	active        bool
	accessLock    sync.Mutex
}

func NewWorker(workerManager *WorkerManager, id int) *Worker {
	return &Worker{
		workerManager: workerManager,
		broker:        workerManager.broker,
		id:            id,
		queue:         workerManager.workerConf.Queue,
		s3Handler: NewS3Handler(workerManager.workerConf.TempDir,
			workerManager.workerConf.MultiUploadThresholdBytes,
			s3entry.NewS3Client(migration.Logger,
				workerManager.workerConf.SrcS3Conf,
				workerManager.httpClient),
			s3entry.NewS3Client(migration.Logger,
				workerManager.workerConf.DstS3Conf,
				workerManager.httpClient),
			migration.Logger),
		active: true,
	}
}

func (w *Worker) Init() bool {
	return true
}

func (w *Worker) setQueue(queue string) {
	w.accessLock.Lock()
	defer w.accessLock.Unlock()
	w.queue = queue
}

func (w *Worker) Run() {
	defer func() {
		if err := recover(); err != nil {
			migration.Logger.Error("migration worker catch a panic",
				zap.Int("workerID", w.id),
				zap.String("Panic", fmt.Sprintf("panic recover: %v", err)))
			panic(err)
		}
		migration.Logger.Error("migration worker exit", zap.Int("workerID", w.id))
		migration.Logger.Sync()
		w.workerManager.workerExitedChannel <- struct{}{}
		w.workerManager.workerWaitGroup.Done()
	}()
	var idleTimes int = 0
	var idlePrintInterval int = 20
	for {
		select {
		case <-w.workerManager.stopWorkersChannel:
			return
		default:
			// 1. Fetch migration item from broker
			migrationItem, err := w.workerManager.broker.GetMigrationItem(w.queue)
			if err != nil {
				migration.Logger.Error("Fetch migration item from broker failed.",
					zap.Int("workerID", w.id),
					zap.String("errorInfo", err.Error()))
				return
			}
			if migrationItem == nil {
				w.active = false
				idleTimes += 1
				if idleTimes%idlePrintInterval == 0 {
					migration.Logger.Info("Current consumer queue is empty",
						zap.Int("WorkerId", w.id))
					idleTimes = 0
				}
				time.Sleep(5 * time.Duration(idleTimes))
				continue
			}
			w.active = true
			// 2. Run migration
			if err = w.s3Handler.runMigrationWithRetry(migrationItem); err != nil {
				migration.Logger.Error("RunMigration failed after retry",
					zap.Int("workerID", w.id),
					zap.Object("migrationInfo", migrationItem),
					zap.String("errorInfo", err.Error()))
				migration.RestoreMigrationItem(migrationItem)
				continue
			}
			// 3. Store migration item information to broker for future check
			if err, ok := w.workerManager.redisHandler.SaddCompletedMigrationItemsWithRetry(migrationItem); !ok {
				migration.Logger.Error("Add completed migration item failed after retry",
					zap.Int("workerID", w.id),
					zap.Object("migrationInfo", migrationItem),
					zap.String("errorInfo", err.Error()))
				migration.RestoreMigrationItem(migrationItem)
				return
			}
		}
	}
}

type WorkerManager struct {
	stopWorkerManagerChannel chan struct{} // used to notify workerManager to shutdown, send by upper caller
	stopWorkersChannel       chan struct{} // used to notify all workers to exit, send by workerManager
	workerExitedChannel      chan struct{} // used to notify workerManager one worker has exited unexpected, send by workers
	workerWaitGroup          sync.WaitGroup

	broker       broker.Broker
	workerConf   *WorkerConf
	httpClient   *http.Client
	workerNums   int
	redisHandler *workerRedisHandler
	workerStatus WorkerStatus
	workers      []*Worker
}

func NewWorkerManager(redisHandler *workerRedisHandler,
	broker broker.Broker, workerConf *WorkerConf, httpClient *http.Client) *WorkerManager {
	return &WorkerManager{
		workerConf:   workerConf,
		broker:       broker,
		redisHandler: redisHandler,
		httpClient:   httpClient,
		workerNums:   workerConf.WorkerNums,
		workerStatus: WORKER_INITIAL,
	}
}

func (w *WorkerManager) Init() bool {
	w.stopWorkerManagerChannel = make(chan struct{}, 1)
	w.stopWorkersChannel = make(chan struct{}, w.workerNums)
	w.workerExitedChannel = make(chan struct{}, w.workerNums)
	w.workers = make([]*Worker, w.workerNums, w.workerNums)
	if !w.createWorker() {
		return false
	}
	w.startRecycleGoroutine()
	return true
}

func (w *WorkerManager) startRecycleGoroutine() {
	go func() {
		defer func() {
			w.workerStatus = WORKER_EXITED
			migration.Logger.Error("workerManager exit successfully.")
			migration.Logger.Sync()
		}()
		for {
			select {
			case <-w.workerExitedChannel:
				w.exit()
				return
			case <-w.stopWorkerManagerChannel:
				w.exit()
				return
			default:
				time.Sleep(time.Second * migration.WAIT_SLEEP_SECONDS)
			}
		}
	}()
}

func (w *WorkerManager) createWorker() bool {
	for id := 0; id < w.workerNums; id++ {
		worker := NewWorker(w, id)
		w.workers[id] = worker
		if !worker.Init() {
			return false
		}
	}
	return true
}

func (w *WorkerManager) StartWorker() {
	w.workerWaitGroup.Add(w.workerConf.WorkerNums)
	for _, worker := range w.workers {
		go func(worker *Worker) {
			worker.Run()
		}(worker)
	}
}

func (w *WorkerManager) getActiveWorkerNums() (res int64) {
	res = 0
	for _, worker := range w.workers {
		if worker.active == true {
			res += 1
		}
	}
	return
}

func (w *WorkerManager) setConsumerQueue(queue string) {
	for _, worker := range w.workers {
		worker.setQueue(queue)
	}
}

func (w *WorkerManager) Exit() {
	w.stopWorkerManagerChannel <- struct{}{}
}

func (w *WorkerManager) exit() {
	for i := 0; i < w.workerNums; i++ {
		w.stopWorkersChannel <- struct{}{}
	}
	w.workerWaitGroup.Wait()
}

func (w *WorkerManager) GetStatus() WorkerStatus {
	return w.workerStatus
}
