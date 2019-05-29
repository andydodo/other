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

type CheckerStatus int

const (
	CHECKER_INITIAL CheckerStatus = iota
	CHECKER_PROCESSING
	CHECKER_EXITED
)

type Checker struct {
	*BackupCommonMethodWrapper
	id                 int
	initDstQueueLength int64
	lastDstQueueIndex  int64
	dstQueue           string

	checkerRedeliverChannel *chan *BackupChannelInfo
	continueCheckerChannel  *chan struct{}
	checkerManager          *CheckerManager
}

func NewChecker(id int, dstQueue string, checkerRedeliverChannel *chan *BackupChannelInfo,
	continueCheckerChannel *chan struct{}, checkerManager *CheckerManager) *Checker {
	return &Checker{
		id:                      id,
		dstQueue:                dstQueue,
		initDstQueueLength:      0,
		lastDstQueueIndex:       -1,
		checkerRedeliverChannel: checkerRedeliverChannel,
		continueCheckerChannel:  continueCheckerChannel,
		checkerManager:          checkerManager,
	}
}

func (c *Checker) Init() bool {
	var ok bool
	c.initDstQueueLength, ok = c.checkerManager.redisHandler.llenDstQueueWithRetry(c.dstQueue)
	if !ok {
		c.checkerManager.logger.Error("llenDstQueueWithRetry failed",
			zap.String("destinationQueue", c.dstQueue))
		return false
	}
	return true
}

func (c *Checker) Run() {
	defer func() {
		if err := recover(); err != nil {
			c.checkerManager.logger.Error("backup checker catch a panic",
				zap.Int("checkerID", c.id),
				zap.String("Panic", fmt.Sprintf("panic recover: %v", err)))
		}
		c.checkerManager.logger.Error("backup Checker exit", zap.Int("checkerID", c.id))
		c.checkerManager.logger.Sync()
		c.checkerManager.checkerExitedChannel <- struct{}{}
		c.checkerManager.checkerWaitGroup.Done()
	}()
	if continueFlag := c.historicalLoop(); !continueFlag {
		return
	}
	if continueFlag := c.conventionalLoop(); !continueFlag {
		return
	}
}

func (c *Checker) conventionalLoop() (res bool) {
	waitSleepTimeCoefficient := 1
	logIdleCount := 0
	for {
		select {
		case <-c.checkerManager.stopCheckerChannel:
			return false
		case <-*c.continueCheckerChannel:
			// 1. Lindex last item to check process status
			backupSourceItem, ok := c.checkerManager.redisHandler.lindexLastItemWithRetry(c.dstQueue, c.lastDstQueueIndex)
			if !ok {
				c.checkerManager.logger.Error("lindexLastItemWithRetry failed after retry",
					zap.String("destinationQueue", c.dstQueue),
					zap.Int64("lastDstQueueIndex", c.lastDstQueueIndex))
				time.Sleep(time.Second * time.Duration(waitSleepTimeCoefficient*WAIT_SLEEP_SECONDS))
				waitSleepTimeCoefficient += 1
				*c.continueCheckerChannel <- struct{}{}
				break
			}
			if backupSourceItem == common.EMPTY_STRING {
				if logIdleCount%LOG_IDLE_THRESHOLD == 0 {
					c.checkerManager.logger.Info("No backup item in processing",
						zap.String("destinationQueue", c.dstQueue))
				}
				if logIdleCount < MAX_IDLE_COUNT {
					logIdleCount += 1
				} else {
					logIdleCount = 0
				}
				time.Sleep(time.Second * WAIT_SLEEP_SECONDS)
				*c.continueCheckerChannel <- struct{}{}
				break
			}
			logIdleCount = 0
			waitSleepTimeCoefficient = 1
			// 2. Parse item to get BackupSourceInfo
			backupChannelInfo, err := c.parseBackupSourceItem(backupSourceItem)
			if err != nil {
				// Failed means backup item with wrong format, skip it.
				c.lastDstQueueIndex -= 1
				c.checkerManager.logger.Error("parseBackupSourceItem faild",
					zap.String("error", err.Error()),
					zap.String("destinationQueue", c.dstQueue),
					zap.Int64("lastDstQueueIndex", c.lastDstQueueIndex))
				*c.continueCheckerChannel <- struct{}{}
				break
			}
			// 3. Check if backup item has expired
			lastUpdateTimeString, ok := c.checkerManager.redisHandler.hgetBackupItemTimeToCheckAliveWithRetry(
				backupChannelInfo.backupSourceInfo)
			if !ok {
				c.checkerManager.logger.Error("hgetBackupItemTimeToCheckAliveWithRetry faild after retry",
					zap.Object("backupSourceInfo", backupChannelInfo.backupSourceInfo))
				return false
			}
			// reach here means dispatcher have no time to update item status yet, sleep for a while and retry
			if lastUpdateTimeString == common.EMPTY_STRING {
				time.Sleep(time.Second * (WAIT_SLEEP_SECONDS + 3))
				lastUpdateTimeString, ok = c.checkerManager.redisHandler.hgetBackupItemTimeToCheckAliveWithRetry(
					backupChannelInfo.backupSourceInfo)
				if !ok {
					c.checkerManager.logger.Error("hgetBackupItemTimeToCheckAliveWithRetry faild after retry",
						zap.Object("backupSourceInfo", backupChannelInfo.backupSourceInfo))
					return false
				}
				// unexpected status, redeliver item, no redeliver DELETE_OPERATION item
				if lastUpdateTimeString == common.EMPTY_STRING {
					if backupChannelInfo.backupSourceInfo.operation == DELETE_OPERATION {
						err = c.checkerManager.redisHandler.RPOPLastItem(c.dstQueue)
						if err != nil {
							c.checkerManager.logger.Error("RPOPLastItem failed",
								zap.String("error", err.Error()),
								zap.String("destinationQueue", c.dstQueue),
								zap.Object("backupSourceInfo", backupChannelInfo.backupSourceInfo))
							*c.continueCheckerChannel <- struct{}{}
							break
						}
						ok = c.checkerManager.redisHandler.hdelBackupItemTimestampWithRetry(backupChannelInfo.backupSourceInfo)
						if !ok {
							c.checkerManager.logger.Error("hdelBackupItemTimestampWithRetry failed",
								zap.Object("backupSourceInfo", backupChannelInfo.backupSourceInfo))
							*c.continueCheckerChannel <- struct{}{}
							break
						}
						c.checkerManager.logger.Info("Drop unexpected item",
							zap.Object("backupSourceInfo", backupChannelInfo.backupSourceInfo))
						*c.continueCheckerChannel <- struct{}{}
						break
					}
					c.checkerManager.logger.Error("Unexpected, redeliver to dispacter",
						zap.Object("backupSourceInfo", backupChannelInfo.backupSourceInfo))
					*c.checkerRedeliverChannel <- backupChannelInfo
					break
				}
			}
			// reach here means item is in queue, wait for a while
			if lastUpdateTimeString == WAITING_TIMESTAMP_STRING {
				c.checkerManager.logger.Info("Item is in waiting status",
					zap.Object("backupSourceInfo", backupChannelInfo.backupSourceInfo),
					zap.Int("CheckerId", c.id))
				time.Sleep(time.Second * WAIT_SLEEP_SECONDS)
				*c.continueCheckerChannel <- struct{}{}
				break
			}
			if lastUpdateTimeString == COMPLETED_TIMESTAMP_STRING {
				// rpop
				err = c.checkerManager.redisHandler.RPOPLastItem(c.dstQueue)
				if err != nil {
					c.checkerManager.logger.Error("RPOPLastItem failed",
						zap.String("error", err.Error()),
						zap.String("destinationQueue", c.dstQueue),
						zap.Object("backupSourceInfo", backupChannelInfo.backupSourceInfo))
					*c.continueCheckerChannel <- struct{}{}
					break
				}
				// del, maybe leak if del failed.
				ok = c.checkerManager.redisHandler.hdelBackupItemTimestampWithRetry(backupChannelInfo.backupSourceInfo)
				if !ok {
					c.checkerManager.logger.Error("hdelBackupItemTimestampWithRetry failed",
						zap.Object("backupSourceInfo", backupChannelInfo.backupSourceInfo))
					*c.continueCheckerChannel <- struct{}{}
					break
				}
				c.checkerManager.logger.Info("Back up successfully",
					zap.Object("backupSourceInfo", backupChannelInfo.backupSourceInfo))
				*c.continueCheckerChannel <- struct{}{}
				break
			}
			lastUpdateTimeInt, err := strconv.ParseInt(lastUpdateTimeString, 10, 64)
			if err != nil {
				c.checkerManager.logger.Error("Redeliver to dispacter",
					zap.String("error", err.Error()),
					zap.String("destinationQueue", c.dstQueue),
					zap.Object("backupSourceInfo", backupChannelInfo.backupSourceInfo))
				*c.checkerRedeliverChannel <- backupChannelInfo
				break
			}
			// backup item is in processing when the last update time is in the survival cycle
			currentTime := time.Now().Unix()
			if (currentTime - lastUpdateTimeInt) < c.checkerManager.survivalCycleTimeSeconds {
				c.checkerManager.logger.Info("Last item is in processing, sleep for a while",
					zap.String("destinationQueue", c.dstQueue),
					zap.Object("backupSourceInfo", backupChannelInfo.backupSourceInfo))
				time.Sleep(time.Second * WAIT_SLEEP_SECONDS)
				*c.continueCheckerChannel <- struct{}{}
				break
			}
			// reach here means backup item is expired which indicate the worker has been exit unexpected,
			// then redeliver the backup item to dispatcher to redo. Do not send to continueCheckerChannel,
			// should be woken up by dispatcher instead to avoid replicate redelivering. In fact, checker
			// will not reach here as worker will redeleiver failed item to dispatcher by itself in most cases.
			if backupChannelInfo.backupSourceInfo.operation == DELETE_OPERATION {
				// no redeliver DELETE_OPERATION item
				err = c.checkerManager.redisHandler.RPOPLastItem(c.dstQueue)
				if err != nil {
					c.checkerManager.logger.Error("RPOPLastItem failed",
						zap.String("error", err.Error()),
						zap.String("destinationQueue", c.dstQueue),
						zap.Object("backupSourceInfo", backupChannelInfo.backupSourceInfo))
					*c.continueCheckerChannel <- struct{}{}
					break
				}
				ok = c.checkerManager.redisHandler.hdelBackupItemTimestampWithRetry(backupChannelInfo.backupSourceInfo)
				if !ok {
					c.checkerManager.logger.Error("hdelBackupItemTimestampWithRetry failed",
						zap.Object("backupSourceInfo", backupChannelInfo.backupSourceInfo))
					*c.continueCheckerChannel <- struct{}{}
					break
				}
				c.checkerManager.logger.Error("Drop failed item",
					zap.String("destinationQueue", c.dstQueue),
					zap.Object("backupSourceInfo", backupChannelInfo.backupSourceInfo))
				*c.continueCheckerChannel <- struct{}{}
				break
			}
			// Redeliver twice (one by worker and one by checker), leak memory.
			c.checkerManager.logger.Error("Last item expired, redeliver to dispatcher, unexpect status",
				zap.String("destinationQueue", c.dstQueue),
				zap.Object("backupSourceInfo", backupChannelInfo.backupSourceInfo))
			*c.checkerRedeliverChannel <- backupChannelInfo
			break
		default:
			c.checkerManager.logger.Info("Checker sleep for dispatcher send continue signal.")
			time.Sleep(time.Second * WAIT_SLEEP_SECONDS)
			break
		}
		c.checkerManager.logger.Sync()
	}
	return false
}

func (c *Checker) historicalLoop() (res bool) {
	var lrangeCountsPerTime int64 = 100
	var start int64 = -lrangeCountsPerTime
	var stop int64 = -1
	waitSleepTimeCoefficient := 1
	for {
		select {
		case <-c.checkerManager.stopCheckerChannel:
			return false
		default:
			// 1. lrange DstQueue to fetch tasks
			backupSourceItems, ok := c.checkerManager.redisHandler.lrangeDstQueueWithRetry(c.dstQueue, start, stop)
			if !ok {
				c.checkerManager.logger.Error("lrangeDstQueueWithRetry failed",
					zap.String("destinationQueue", c.dstQueue),
					zap.Int64("startIndex", start),
					zap.Int64("stopIndex", stop))
				time.Sleep(time.Second * time.Duration(waitSleepTimeCoefficient*WAIT_SLEEP_SECONDS))
				waitSleepTimeCoefficient += 1
				break
			}
			// 2. redeleiver to dispatcher
			for _, backupSourceItem := range backupSourceItems {
				// Parse item to BackupSourceInfo
				backupChannelInfo, err := c.parseBackupSourceItem(backupSourceItem)
				if err != nil {
					c.checkerManager.logger.Error("parseBackupSourceItem faild",
						zap.String("backupSourceItem", backupSourceItem))
					continue
				}
				if backupChannelInfo.backupSourceInfo.operation == DELETE_OPERATION {
					c.checkerManager.logger.Info("Skip historical item which with delete operation",
						zap.Object("backupSourceInfo", backupChannelInfo.backupSourceInfo))
					// Mark backup item with expired time to rpop in future
					ok = c.checkerManager.redisHandler.hsetBackupItemTimeToKeepAliveWithRetry(
						backupChannelInfo.backupSourceInfo, "0")
					if !ok {
						c.checkerManager.logger.Error("Mark historical delete operation item with expired time failed",
							zap.Object("backupSourceInfo", backupChannelInfo.backupSourceInfo),
							zap.Int("checkerId", c.id))
						return false
					}
					continue
				}
				// redeliver to dispatcher if it has not been completed.
				lastUpdateTimeString, ok := c.checkerManager.redisHandler.hgetBackupItemTimeToCheckAliveWithRetry(
					backupChannelInfo.backupSourceInfo)
				if !ok {
					c.checkerManager.logger.Error("hgetBackupItemTimeToCheckAliveWithRetry faild",
						zap.Object("backupSourceInfo", backupChannelInfo.backupSourceInfo))
					return false
				}
				if lastUpdateTimeString != COMPLETED_TIMESTAMP_STRING {
					*c.checkerRedeliverChannel <- backupChannelInfo
					c.checkerManager.logger.Info("Redeliver historical backup item to dispatcher",
						zap.Object("backupSourceInfo", backupChannelInfo.backupSourceInfo))
					continue
				}
			}
			waitSleepTimeCoefficient = 1
		}
		// update index
		if util.Abs(start) >= c.initDstQueueLength {
			c.checkerManager.logger.Info("Historical items all have been redelivered to dispatcher",
				zap.String("queue", c.dstQueue),
				zap.Int("checkerID", c.id))
			break
		}
		start -= lrangeCountsPerTime
		stop -= lrangeCountsPerTime
	}
	// close channel to notify dispatcher that all historical items have been redelivered
	close(*c.checkerRedeliverChannel)
	return true
}

type CheckerManager struct {
	*BackupCommonMethodWrapper
	stopCheckerChannel          chan struct{} // passive shutdown
	checkerManagerExitedChannel chan struct{} // used to notify main goroutine that program should stop
	checkerExitedChannel        chan struct{} // active shutdown
	checkerWaitGroup            sync.WaitGroup

	exitLock                 sync.Mutex
	checkerStatus            CheckerStatus
	logger                   *zap.Logger
	backupConf               *BackupConf
	checkers                 []*Checker
	checkerNums              int
	redisHandler             *BackupRedisHandler
	survivalCycleTimeSeconds int64

	checkerRedeliverChannel []chan *BackupChannelInfo
	continueCheckerChannel  []chan struct{}
}

func NewCheckerManager(logger *zap.Logger, backupConf *BackupConf, redisHandler *BackupRedisHandler,
	checkerRedeliverChannel []chan *BackupChannelInfo, continueCheckerChannel []chan struct{}) *CheckerManager {
	return &CheckerManager{
		logger:                   logger,
		backupConf:               backupConf,
		checkerNums:              backupConf.QueueShardingNumber,
		redisHandler:             redisHandler,
		survivalCycleTimeSeconds: backupConf.KeepAliveSleepTimeSeconds + 5,
		checkerRedeliverChannel:  checkerRedeliverChannel,
		continueCheckerChannel:   continueCheckerChannel,
		checkerStatus:            CHECKER_INITIAL,
	}
}

func (c *CheckerManager) createChecker() bool {
	srcQueuePrefix := c.backupConf.QueuePrefix
	dstQueuePrefix := c.makeDstQueuePrefix(srcQueuePrefix)
	if c.checkerNums == 1 {
		checker := NewChecker(0,
			dstQueuePrefix,
			&c.checkerRedeliverChannel[0],
			&c.continueCheckerChannel[0],
			c)
		c.checkers[0] = checker
		return checker.Init()
	} else {
		for id := 0; id < c.checkerNums; id++ {
			checker := NewChecker(id,
				dstQueuePrefix+STRING_GLUE+strconv.Itoa(id),
				&c.checkerRedeliverChannel[id],
				&c.continueCheckerChannel[id],
				c)
			c.checkers[id] = checker
			if !checker.Init() {
				return false
			}
		}
		return true
	}
}

func (c *CheckerManager) Init() bool {
	c.stopCheckerChannel = make(chan struct{}, c.checkerNums*3)
	c.checkerExitedChannel = make(chan struct{}, c.checkerNums)
	c.checkerManagerExitedChannel = make(chan struct{}, 1)
	c.checkers = make([]*Checker, c.checkerNums, c.checkerNums)
	if !c.createChecker() {
		return false
	}
	c.startRecycleGoroutine()
	return true
}

func (c *CheckerManager) startRecycleGoroutine() {
	go func() {
		defer func() {
			c.checkerManagerExitedChannel <- struct{}{}
		}()
		for {
			select {
			case <-c.checkerExitedChannel:
				c.Exit()
				return
			default:
				time.Sleep(time.Second * WAIT_SLEEP_SECONDS)
			}
		}
	}()
}

func (c *CheckerManager) StartChecker() {
	for _, checker := range c.checkers {
		go func(checker *Checker) {
			checker.Run()
		}(checker)
		c.checkerWaitGroup.Add(1)
	}
}

func (c *CheckerManager) Exit() {
	c.exitLock.Lock()
	defer c.exitLock.Unlock()
	if c.checkerStatus == CHECKER_EXITED {
		return
	}
	for i := 0; i < c.checkerNums; i++ {
		c.stopCheckerChannel <- struct{}{}
	}
	c.checkerWaitGroup.Wait()
	c.checkerStatus = CHECKER_EXITED
	c.logger.Error("CheckerManager exit successfully")
}
