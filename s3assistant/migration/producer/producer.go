package producer

import (
	"fmt"
	"regexp"
	"s3assistant/common"
	"s3assistant/migration"
	"s3assistant/migration/broker"
	"s3assistant/util"
	"sync"
	"time"

	"go.uber.org/zap"
)

// Producer is a common struct for all producers,
// implemented serveral public methods
type Producer struct {
	*producerRedisHandler
	broker.Broker

	srcBucket, dstBucket string        // the buckets producer in charge
	queue                string        // the queue for store production
	shutDownChannel      chan struct{} // Passive
	stopMutex            sync.Mutex
	status               producerStatus
	exitedChannel        chan struct{}
}

func NewProducer(producerRedisHandler *producerRedisHandler,
	broker broker.Broker, srcBucket, dstBucket, queue string) *Producer {
	return &Producer{
		producerRedisHandler: producerRedisHandler,
		Broker:               broker,
		srcBucket:            srcBucket,
		dstBucket:            dstBucket,
		queue:                queue,
		status:               P_NONE,
	}
}

// ValidateResource the validation about the resources
func (p *Producer) ValidateResource() error {
	return p.producerRedisHandler.ValidateRedisConnection()
}

// InitialProduce initialize the scan cursor and stop channel
func (p *Producer) InitialProduce() (error, bool) {
	cursor, continueFlag := p.GetScanCursorWithRetry()
	if !continueFlag {
		return &util.RedisError{
			Message: "GetScanCursorWithRetry failed",
		}, false
	}
	if p.producerRedisHandler.Flag.String() == "check" && cursor == common.EMPTY_STRING {
		// Sdiff added and completed set to fetch incompleted migration items
		num, continueFlag := p.SdiffStoreAddedAddCompletedMigrationItems()
		if !continueFlag {
			return &util.RedisError{
				Message: "SdiffStoreAddedAndCompletedMigrationItemsWithRetry failed",
			}, false
		}
		// return true to mark there are no other items need to migrate
		if num == 0 {
			return nil, true
		}
	}
	p.shutDownChannel = make(chan struct{}, 1)
	p.exitedChannel = make(chan struct{}, 1)
	p.status = P_INITIALIZED
	p.startRecycleGoroutine()
	return nil, false
}

// StartProduce, a common part of StartProduce, which is responsible for processing
// the meta information of migration item.
func (p *Producer) StartProduce(objectNames []string) bool {
	// 1. Store information to broker
	var ms []*migration.MigrationItem = make([]*migration.MigrationItem, len(objectNames))
	for i, objectName := range objectNames {
		ms[i] = migration.FetchMigrationItem()
		ms[i].SrcBucketName = p.srcBucket
		ms[i].DstBucketName = p.dstBucket
		// add by andy
		re := regexp.MustCompile(`\s`)
		if ok := re.MatchString(objectName); ok {
			objectName = re.ReplaceAllString(objectName, "-")
			ms[i].ObjectName = objectName
		} else {
			ms[i].ObjectName = objectName
		}
	}
	fmt.Printf("switch to migration, len: %d\n", len(objectNames))
	// 1.1 Store migration items to broker queue
	if res := func() bool {
		defer migration.RestoreMigrationItem(ms...)
		fmt.Printf("queue: %s\n", p.queue)
		if err := p.Broker.SendMigrationItem(p.queue, ms); err != nil {
			migration.Logger.Error("SendMigrationItem failed.",
				zap.String("errorInfo", err.Error()))
			return false
		}
		return true
	}(); !res {
		return false
	}
	fmt.Println("send to migration")
	// 1.2 Store object name to added set for future checking
	if err, _ := p.producerRedisHandler.SaddAddedMigrationItemsWithRetry(objectNames...); err != nil {
		migration.Logger.Error("SaddAddedMigrationItemsWithRetry failed.",
			zap.String("errorInfo", err.Error()))
		return false
	}
	// 2. Update cursor after all objects have been added to broker
	// and the all related information has been recorded
	if err, _ := p.producerRedisHandler.SetScanCursorWithRetry(); err != nil {
		migration.Logger.Error("SetScanCursorWithRetry failed.",
			zap.String("errorInfo", err.Error()))
		return false
	}
	migration.Logger.Info("Add objects success",
		zap.Int("counts", len(objectNames)))
	// 3. Judge if all objects have been added to broker
	nextCursor, continueFlag := p.producerRedisHandler.GetScanCursorWithRetry()
	if !continueFlag {
		migration.Logger.Error("GetScanCursorWithRetry failed.")
		return false
	}
	if nextCursor == common.EMPTY_STRING || nextCursor == "0" {
		migration.Logger.Info("All objects have been added to broker.")
		// Del the cursor
		if err, _ := p.producerRedisHandler.DelScanCursorWithRetry(); err != nil {
			migration.Logger.Error("DelScanCursorWithRetry failed.")
		}
		return false
	}
	return true
}

// StopProduce stop the producer, send a channel to producer
// and wait until producer down
func (p *Producer) StopProduce() {
	p.shutDownChannel <- struct{}{}
}

func (p *Producer) startRecycleGoroutine() {
	go func() {
		defer p.markStopped()
		for {
			select {
			case <-p.exitedChannel:
				return
			default:
				time.Sleep(5)
				continue
			}
		}
	}()
}

// markStopped used to mark producer status to stopped
func (p *Producer) markStopped() {
	p.stopMutex.Lock()
	defer p.stopMutex.Unlock()
	p.status = P_STOPPED
}

// getStatus get the producer status
func (p *Producer) getStatus() producerStatus {
	p.stopMutex.Lock()
	defer p.stopMutex.Unlock()
	return p.status
}
