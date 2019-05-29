package backup

import (
	"fmt"
	"s3assistant/common"
	"s3assistant/util"
	"strconv"
	"sync"
	"time"

	"go.uber.org/zap"
)

type DispatcherStatus int

const (
	DISPATCHER_INITIAL DispatcherStatus = iota
	DISPATCHER_PROCESSING
	DISPATCHER_EXITED
)

type Dispatcher struct {
	*BackupCommonMethodWrapper
	id                 int
	srcQueue, dstQueue string

	checkerRedeliverChannel *chan *BackupChannelInfo
	workerRedeliverChannel  *chan *BackupChannelInfo
	continueCheckerChannel  *chan struct{}
	dispatcherManager       *DispatcherManager
}

func NewDispatcher(id int, srcQueue, dstQueue string,
	checkerRedeliverChannel, workerRedeliverChannel *chan *BackupChannelInfo,
	continueCheckerChannel *chan struct{}, dispatcherManager *DispatcherManager) *Dispatcher {
	return &Dispatcher{
		id:                      id,
		srcQueue:                srcQueue,
		dstQueue:                dstQueue,
		checkerRedeliverChannel: checkerRedeliverChannel,
		workerRedeliverChannel:  workerRedeliverChannel,
		continueCheckerChannel:  continueCheckerChannel,
		dispatcherManager:       dispatcherManager,
	}
}

func (d *Dispatcher) Init() bool {
	return true
}

func (d *Dispatcher) Run() {
	defer func() {
		if err := recover(); err != nil {
			d.dispatcherManager.logger.Error("backup dispatcher catch a panic",
				zap.Int("dispatcherID", d.id),
				zap.String("Panic", fmt.Sprintf("panic recover: %v", err)))
		}
		d.dispatcherManager.logger.Error("backup Dispatcher exit", zap.Int("dispatcherID", d.id))
		d.dispatcherManager.logger.Sync()
		d.dispatcherManager.dispatcherExitedChannel <- struct{}{}
		d.dispatcherManager.dispatcherWaitGroup.Done()
	}()
	if continueFlag := d.historicalLoop(); !continueFlag {
		return
	}
	if continueFlag := d.conventionalLoop(); !continueFlag {
		return
	}
}

func (d *Dispatcher) conventionalLoop() bool {
	continueDispatcherChannel := make(chan struct{}, 1)
	if d.dispatcherManager.backupConf.StartDispatcher == true {
		continueDispatcherChannel <- struct{}{}
	}
	waitSleepTimeCoefficient := 1
	logIdleCount := 0
	for {
		select {
		case <-d.dispatcherManager.stopDispatcherChannel:
			return false
		case <-continueDispatcherChannel:
			// 1. RPOPLPUSH
			backupSourceItem, err := d.dispatcherManager.redisHandler.RPOPLPUSHBackupItem(d.srcQueue, d.dstQueue)
			if err != nil {
				d.dispatcherManager.logger.Error("RPOPLPUSHBackupItem failed", zap.String("error", err.Error()))
				time.Sleep(time.Second * time.Duration(waitSleepTimeCoefficient*WAIT_SLEEP_SECONDS))
				waitSleepTimeCoefficient += 1
				continueDispatcherChannel <- struct{}{}
				break
			}
			if backupSourceItem == common.EMPTY_STRING {
				if logIdleCount%LOG_IDLE_THRESHOLD == 0 {
					d.dispatcherManager.logger.Info("There are no backupSourceItems in queue",
						zap.String("queue", d.srcQueue))
				}
				if logIdleCount < MAX_IDLE_COUNT {
					logIdleCount += 1
				} else {
					logIdleCount = 0
				}
				time.Sleep(time.Second * WAIT_SLEEP_SECONDS)
				continueDispatcherChannel <- struct{}{}
				break
			}
			// 2. Parse item
			backupChannelInfo, err := d.parseBackupSourceItem(backupSourceItem)
			if err != nil {
				d.dispatcherManager.logger.Error("parseBackupSourceItem faild",
					zap.String("error", err.Error()),
					zap.String("backupSourceItem", backupSourceItem))
				continueDispatcherChannel <- struct{}{}
				break
			}
			// checker may redeliver to dispatcher when dispatcher have no time to mark status
			ok := d.dispatcherManager.redisHandler.hsetBackupItemTimeToKeepAliveWithRetry(
				backupChannelInfo.backupSourceInfo, WAITING_TIMESTAMP_STRING)
			if !ok {
				d.dispatcherManager.logger.Error("hsetBackupItemTimeToKeepAliveWithRetry failed, mark waiting status failed",
					zap.Object("backupSourceInfo", backupChannelInfo.backupSourceInfo))
				return false
			}
			// 3. Send to worker
			if ok := d.sendBackupChannelInfoToWorker(backupChannelInfo, "new item"); !ok {
				return false
			}
			logIdleCount = 0
			waitSleepTimeCoefficient = 1
			continueDispatcherChannel <- struct{}{}
			break
		case backupChannelInfo := <-*d.workerRedeliverChannel:
			d.dispatcherManager.logger.Info("Dispatcher has received redelivered item from worker",
				zap.Object("backupSourceInfo", backupChannelInfo.backupSourceInfo))
			ok := d.dispatcherManager.redisHandler.hsetBackupItemTimeToKeepAliveWithRetry(
				backupChannelInfo.backupSourceInfo, WAITING_TIMESTAMP_STRING)
			if !ok {
				d.dispatcherManager.logger.Error("hsetBackupItemTimeToKeepAliveWithRetry failed, mark waiting status failed",
					zap.Object("backupSourceInfo", backupChannelInfo.backupSourceInfo))
				return false
			}
			if ok := d.sendBackupChannelInfoToWorker(backupChannelInfo, "redelivered by worker"); !ok {
				return false
			}
			break
		case backupChannelInfo := <-*d.checkerRedeliverChannel:
			d.dispatcherManager.logger.Info("Dispatcher has received redelivered item from checker",
				zap.Object("backupSourceInfo", backupChannelInfo.backupSourceInfo))
			ok := d.dispatcherManager.redisHandler.hsetBackupItemTimeToKeepAliveWithRetry(
				backupChannelInfo.backupSourceInfo, WAITING_TIMESTAMP_STRING)
			if !ok {
				d.dispatcherManager.logger.Error("hsetBackupItemTimeToKeepAliveWithRetry failed, mark wait status failed",
					zap.Object("backupSourceInfo", backupChannelInfo.backupSourceInfo))
				return false
			}
			if ok := d.sendBackupChannelInfoToWorker(backupChannelInfo, "redelivered by checker"); !ok {
				return false
			}
			*d.continueCheckerChannel <- struct{}{}
			break
		// reach here when all channel are blocked, means:
		// 1. StartDispatcher equals false
		// 2. No redeliver event occur
		default:
			// reach here when StartDispatcher is set to false, then the dispatcher should only be responsible for
			// redelivering failed(expired) backup item to worker
			d.dispatcherManager.logger.Info("Waiting for completion")
			time.Sleep(time.Second * WAIT_SLEEP_SECONDS)
			break
		}
		d.dispatcherManager.logger.Sync()
	}
	return false
}

func (d *Dispatcher) historicalLoop() bool {
	redeliverHistoricalItemsCompleted := false
	for {
		if redeliverHistoricalItemsCompleted == true {
			break
		}
		select {
		case <-d.dispatcherManager.stopDispatcherChannel:
			return false
		case backupChannelInfo, ok := <-*d.checkerRedeliverChannel:
			if !ok {
				// when checkerRedeliverChannel is closed means all historical items have been redelivered
				redeliverHistoricalItemsCompleted = true
				break
			}
			// Dispatcher may blocked here when wroker's job queue is full, and if worker also blocked when
			// workerRedeliverChannel is full, the program will blocked forever(dead lock), restart program in this case.
			d.dispatcherManager.logger.Info("Dispatcher has received historical backup item",
				zap.Int("dispatcherID", d.id), zap.Object("backupSourceInfo", backupChannelInfo.backupSourceInfo))
			if ok = d.sendBackupChannelInfoToWorker(backupChannelInfo, "historical item"); !ok {
				return false
			}
			// mark item status to WAITING_TIMESTAMP_STRING
			ok = d.dispatcherManager.redisHandler.hsetBackupItemTimeToKeepAliveWithRetry(
				backupChannelInfo.backupSourceInfo, WAITING_TIMESTAMP_STRING)
			if !ok {
				d.dispatcherManager.logger.Error("hsetBackupItemTimeToKeepAliveWithRetry failed, mark waiting status failed",
					zap.Object("backupSourceInfo", backupChannelInfo.backupSourceInfo))
				return false
			}
			break
		default:
			break
		}
	}
	// After redeliver all historical items to worker, then start normal checker circuit
	// And reopen checkerRedeliverChannel
	*d.checkerRedeliverChannel = make(chan *BackupChannelInfo, CHANNEL_BUFFER_SIZE)
	*d.continueCheckerChannel <- struct{}{}
	return true
}

func (d *Dispatcher) sendBackupChannelInfoToWorker(backupChannelInfo *BackupChannelInfo, message string) (res bool) {
	backupChannelInfo.dispatcherID = d.id
	workerID := int(util.Hash(backupChannelInfo.backupSourceInfo.objectName)) % d.dispatcherManager.backupConf.WorkerNums
	// Catch the panic to judge if worker has exited or not
	defer func() {
		if r := recover(); r != nil {
			d.dispatcherManager.logger.Error("send to worker failed, worker has exited",
				zap.Int("dispatcherID", d.id),
				zap.Int("workerID", workerID))
			res = false
		}
	}()
	for {
		select {
		case d.dispatcherManager.workerAndQueue[workerID] <- backupChannelInfo:
			d.dispatcherManager.logger.Info("Dispatcher delivers backup item to worker successfully",
				zap.Object("backupSourceInfo", backupChannelInfo.backupSourceInfo),
				zap.Int("workerID", workerID),
				zap.Int("dispatcherID", d.id),
				zap.String("message", message))
			return true
		default:
			d.dispatcherManager.logger.Error("Worker queue is full, sleep for a while",
				zap.Int("dispatcherID", d.id),
				zap.Int("workerID", workerID))
			time.Sleep(time.Second * WAIT_SLEEP_SECONDS)
			break
		}
	}
	return true
}

type DispatcherManager struct {
	*BackupCommonMethodWrapper
	dispatcherNums                 int
	stopDispatcherChannel          chan struct{}
	dispatcherManagerExitedChannel chan struct{}
	dispatcherExitedChannel        chan struct{}
	dispatcherWaitGroup            sync.WaitGroup

	exitLock         sync.Mutex
	dispatcherStatus DispatcherStatus
	logger           *zap.Logger
	backupConf       *BackupConf
	dispatchers      []*Dispatcher
	redisHandler     *BackupRedisHandler
	// the following fields holds the information used to communicate among dispatcher
	// , checker and worker
	workerAndQueue          []chan *BackupChannelInfo
	checkerRedeliverChannel []chan *BackupChannelInfo
	workerRedeliverChannel  []chan *BackupChannelInfo
	continueCheckerChannel  []chan struct{}
}

func NewDispatcherManager(logger *zap.Logger, backupConf *BackupConf, redisHandler *BackupRedisHandler,
	workerAndQueue, checkerRedeliverChannel, workerRedeliverChannel []chan *BackupChannelInfo,
	continueCheckerChannel []chan struct{}) *DispatcherManager {
	return &DispatcherManager{
		logger:                  logger,
		backupConf:              backupConf,
		redisHandler:            redisHandler,
		workerAndQueue:          workerAndQueue,
		checkerRedeliverChannel: checkerRedeliverChannel,
		workerRedeliverChannel:  workerRedeliverChannel,
		continueCheckerChannel:  continueCheckerChannel,
		dispatcherNums:          backupConf.QueueShardingNumber,
		dispatcherStatus:        DISPATCHER_INITIAL,
	}
}

func (d *DispatcherManager) createDispatcher() bool {
	srcQueuePrefix := d.backupConf.QueuePrefix
	dstQueuePrefix := d.makeDstQueuePrefix(srcQueuePrefix)
	if d.dispatcherNums == 1 {
		dispatcher := NewDispatcher(0,
			srcQueuePrefix,
			dstQueuePrefix,
			&d.checkerRedeliverChannel[0],
			&d.workerRedeliverChannel[0],
			&d.continueCheckerChannel[0],
			d)
		d.dispatchers[0] = dispatcher
		return dispatcher.Init()
	} else {
		for id := 0; id < d.dispatcherNums; id++ {
			dispatcher := NewDispatcher(id,
				srcQueuePrefix+STRING_GLUE+strconv.Itoa(id),
				dstQueuePrefix+STRING_GLUE+strconv.Itoa(id),
				&d.checkerRedeliverChannel[id],
				&d.workerRedeliverChannel[id],
				&d.continueCheckerChannel[id],
				d)
			d.dispatchers[id] = dispatcher
			if !dispatcher.Init() {
				return false
			}
		}
		return true
	}
}

func (d *DispatcherManager) startRecycleGoroutine() {
	go func() {
		// stop program when any dispatcher exited
		defer func() {
			d.dispatcherManagerExitedChannel <- struct{}{}
		}()
		for {
			select {
			case <-d.dispatcherExitedChannel:
				d.Exit()
				return
			default:
				time.Sleep(time.Second * WAIT_SLEEP_SECONDS)
			}
		}
	}()
}

func (d *DispatcherManager) Init() bool {
	d.stopDispatcherChannel = make(chan struct{}, d.dispatcherNums*3)
	d.dispatcherExitedChannel = make(chan struct{}, d.dispatcherNums)
	d.dispatchers = make([]*Dispatcher, d.dispatcherNums, d.dispatcherNums)
	d.dispatcherManagerExitedChannel = make(chan struct{}, 1)
	if !d.createDispatcher() {
		return false
	}
	d.startRecycleGoroutine()
	return true
}

func (d *DispatcherManager) StartDispatcher() {
	for _, dispatcher := range d.dispatchers {
		go func(dispatcher *Dispatcher) {
			dispatcher.Run()
		}(dispatcher)
		d.dispatcherWaitGroup.Add(1)
	}
}

func (d *DispatcherManager) Exit() {
	d.exitLock.Lock()
	defer d.exitLock.Unlock()
	if d.dispatcherStatus == DISPATCHER_EXITED {
		return
	}
	for i := 0; i < d.dispatcherNums; i++ {
		d.stopDispatcherChannel <- struct{}{}
	}
	d.dispatcherWaitGroup.Wait()
	d.dispatcherStatus = DISPATCHER_EXITED
	d.logger.Error("DispatcherManager exit successfully")
}
