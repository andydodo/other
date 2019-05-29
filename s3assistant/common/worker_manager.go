package common

import (
	"sync"
	"time"

	"go.uber.org/zap"
)

const (
	WAIT_SLEEP_SECONDS = 1
)

type WorkerStatus int

const (
	WORKER_INITIAL WorkerStatus = iota
	WORKER_PROCESSING
	WORKER_EXITED
)

type WorkerManager struct {
	WorkerNums                 int
	StopWorkerChannel          chan struct{}
	WorkerManagerExitedChannel chan struct{}
	WorkerExitedChannel        chan struct{}
	WorkerWaitGroup            sync.WaitGroup
	WorkerStatus               WorkerStatus
	logger                     *zap.Logger
	exitLock                   sync.Mutex
}

func NewWorkerManager(logger *zap.Logger, workerNums int) *WorkerManager {
	return &WorkerManager{
		logger:       logger,
		WorkerNums:   workerNums,
		WorkerStatus: WORKER_INITIAL,
	}
}
func (w *WorkerManager) Init() {
	w.StopWorkerChannel = make(chan struct{}, w.WorkerNums)
	w.WorkerExitedChannel = make(chan struct{}, w.WorkerNums)
	w.WorkerManagerExitedChannel = make(chan struct{}, 1)
	w.WorkerWaitGroup.Add(w.WorkerNums)
	go func() {
		defer func() {
			w.WorkerStatus = WORKER_EXITED
			w.logger.Error("WorkerManager exit successfully.")
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

func (w *WorkerManager) Exit() {
	w.exitLock.Lock()
	defer w.exitLock.Unlock()
	if w.WorkerStatus == WORKER_EXITED {
		return
	}
	for i := 0; i < w.WorkerNums; i++ {
		w.StopWorkerChannel <- struct{}{}
	}
	w.WorkerWaitGroup.Wait()
}

func (w *WorkerManager) Status() WorkerStatus {
	return w.WorkerStatus
}
