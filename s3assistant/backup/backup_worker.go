package backup

import (
	"context"
	"fmt"
	"net/http"
	"s3assistant/s3entry"
	"sync"
	"time"

	"go.uber.org/zap"
)

const FAILED_QUEUE_INITIAL_CAP = 1024 * 1024

type WorkerStatus int

const (
	WORKER_INITIAL WorkerStatus = iota
	WORKER_PROCESSING
	WORKER_EXITED
)

type Worker struct {
	*BackupCommonMethodWrapper
	id                     int
	queue                  *chan *BackupChannelInfo
	httpClient             *http.Client
	workerRedeliverChannel []chan *BackupChannelInfo
	workerManager          *WorkerManager

	// used to store failed item which can not be redelivered to dispatcher right now
	// when dispatcher has no time to fetch the failed item
	failedQueue chan *BackupChannelInfo
	failedLock  sync.Mutex
	cancel      context.CancelFunc
}

func NewWorker(id int, queue *chan *BackupChannelInfo, httpClient *http.Client,
	workerRedeliverChannel []chan *BackupChannelInfo, workerManager *WorkerManager) *Worker {
	return &Worker{
		id:                     id,
		queue:                  queue,
		httpClient:             httpClient,
		workerRedeliverChannel: workerRedeliverChannel,
		workerManager:          workerManager,
		failedQueue:            make(chan *BackupChannelInfo, FAILED_QUEUE_INITIAL_CAP),
	}
}

func (w *Worker) Init() bool {
	return true
}

func (w *Worker) Run() {
	defer func() {
		if err := recover(); err != nil {
			w.workerManager.logger.Error("backup worker catch a panic",
				zap.Int("workerID", w.id),
				zap.String("Panic", fmt.Sprintf("panic recover: %v", err)))
		}
		w.workerManager.logger.Error("backup worker exit", zap.Int("workerID", w.id))
		w.workerManager.logger.Sync()
		w.workerManager.WorkerExitedChannel <- struct{}{}
		w.workerManager.WorkerWaitGroup.Done()
		w.cancel()
		close(*w.queue)
	}()
	if continueFlag := w.conventionalLoop(); !continueFlag {
		return
	}
}

// startRecycleGoroutine starts a goroutine to redeliver failed backup
// item to dispatcher
func (w *Worker) startRedeliverGoroutine() context.CancelFunc {
	ctx, cancel := context.WithCancel(context.Background())
	go func(ctx context.Context) {
		defer w.workerManager.logger.Sync()
		for {
			select {
			case <-ctx.Done():
				return
			case backupChannelInfo := <-w.failedQueue:
				haveRedelivered := false
				for {
					if haveRedelivered {
						break
					}
					select {
					case <-ctx.Done():
						return
					case w.workerRedeliverChannel[backupChannelInfo.dispatcherID] <- backupChannelInfo:
						w.workerManager.logger.Info("Redeliver failed backup item to dispatcher success",
							zap.Object("backupSourceInfo", backupChannelInfo.backupSourceInfo),
							zap.Int("DispatcherId", backupChannelInfo.dispatcherID),
							zap.Int("WorkerId", w.id))
						haveRedelivered = true
						break
					default:
						w.workerManager.logger.Warn("Redeliver failed backup item to dispatcher failed, retry later",
							zap.Object("backupSourceInfo", backupChannelInfo.backupSourceInfo),
							zap.Int("DispatcherId", backupChannelInfo.dispatcherID),
							zap.Int("WorkerId", w.id))
						time.Sleep(time.Second * WAIT_SLEEP_SECONDS)
						break
					}
				}
			default:
				w.workerManager.logger.Debug("No failed backup item need to redeliver to dispatcher",
					zap.Int("WorkerId", w.id))
				time.Sleep(time.Second * WAIT_SLEEP_SECONDS)
				break
			}
		}
	}(ctx)
	return cancel
}

func (w *Worker) conventionalLoop() (res bool) {
	w.cancel = w.startRedeliverGoroutine()
	s3Handler := NewS3Handler(w.workerManager.logger,
		w.workerManager.backupConf.DestinationBucket,
		w.workerManager.backupConf.TempFileDir,
		w.workerManager.backupConf.MinistPartSizeBytes,
		w.workerManager.backupConf.KeepAliveSleepTimeSeconds,
		s3entry.NewS3Client(w.workerManager.logger, w.workerManager.backupConf.SrcS3Conf, w.httpClient),
		s3entry.NewS3Client(w.workerManager.logger, w.workerManager.backupConf.DstS3Conf, w.httpClient))
	for {
		select {
		case backupChannelInfo := <-*w.queue:
			w.workerManager.logger.Debug("Begin to handle backup item",
				zap.Object("backupSourceInfo", backupChannelInfo.backupSourceInfo),
				zap.Int("WorkerId", w.id))
			if ok := func() (res bool) {
				if err := s3Handler.RunBackup(backupChannelInfo.backupSourceInfo, w.workerManager.redisHandler); err != nil {
					w.workerManager.logger.Error("Handle backup failed after retry",
						zap.Object("backupSourceInfo", backupChannelInfo.backupSourceInfo))
					return false
				}
				// Put backupSourceInfo back to pool if success
				restoreBackupChannelInfo(backupChannelInfo)
				return true
			}(); !ok {
				// Store in failedQueue for future redeliver
				if backupChannelInfo.backupSourceInfo.operation != DELETE_OPERATION {
					w.workerManager.logger.Debug("Begin to store backup item for future redeliver",
						zap.Object("backupSourceInfo", backupChannelInfo.backupSourceInfo),
						zap.Int("WorkerId", w.id))
					// May blocked in here when failedQueue is full
					w.failedQueue <- backupChannelInfo
				}
			}
			break
		case <-w.workerManager.StopWorkerChannel:
			return
		}
		w.workerManager.logger.Sync()
	}
}

type WorkerManager struct {
	StopWorkerChannel          chan struct{}
	WorkerManagerExitedChannel chan struct{}
	WorkerExitedChannel        chan struct{}
	WorkerWaitGroup            sync.WaitGroup
	workerStatus               WorkerStatus

	logger       *zap.Logger
	backupConf   *BackupConf
	workerNums   int
	redisHandler *BackupRedisHandler
	httpClient   *http.Client

	exitLock sync.Mutex
	workers  []*Worker

	workerAndQueue         []chan *BackupChannelInfo
	workerRedeliverChannel []chan *BackupChannelInfo
}

func NewWorkerManager(logger *zap.Logger, backupConf *BackupConf, redisHandler *BackupRedisHandler,
	httpClient *http.Client, workerAndQueue, workerRedeliverChannel []chan *BackupChannelInfo) *WorkerManager {
	return &WorkerManager{
		logger:                 logger,
		backupConf:             backupConf,
		redisHandler:           redisHandler,
		httpClient:             httpClient,
		workerNums:             backupConf.WorkerNums,
		workerAndQueue:         workerAndQueue,
		workerRedeliverChannel: workerRedeliverChannel,
		workerStatus:           WORKER_INITIAL,
	}
}

func (w *WorkerManager) Init() bool {
	w.StopWorkerChannel = make(chan struct{}, w.workerNums*3)
	w.WorkerExitedChannel = make(chan struct{}, w.workerNums)
	w.WorkerManagerExitedChannel = make(chan struct{}, 1)
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
			w.WorkerManagerExitedChannel <- struct{}{}
		}()
		for {
			select {
			case <-w.WorkerExitedChannel:
				w.Exit()
				return
			default:
				time.Sleep(time.Second * WAIT_SLEEP_SECONDS)
			}
		}
	}()
}

func (w *WorkerManager) createWorker() bool {
	for id := 0; id < w.workerNums; id++ {
		worker := NewWorker(id,
			&w.workerAndQueue[id],
			w.httpClient,
			w.workerRedeliverChannel,
			w)
		w.workers[id] = worker
		if !worker.Init() {
			return false
		}
	}
	return true
}

func (w *WorkerManager) StartWorker() {
	for _, worker := range w.workers {
		go func(worker *Worker) {
			worker.Run()
		}(worker)
		w.WorkerWaitGroup.Add(1)
	}
}

func (w *WorkerManager) Exit() {
	w.exitLock.Lock()
	defer w.exitLock.Unlock()
	if w.workerStatus == WORKER_EXITED {
		return
	}
	for i := 0; i < w.workerNums; i++ {
		w.StopWorkerChannel <- struct{}{}
	}
	w.WorkerWaitGroup.Wait()
	w.workerStatus = WORKER_EXITED
	w.logger.Error("WorkerManager exit successfully")
}

func (w *WorkerManager) Status() WorkerStatus {
	return w.workerStatus
}
