package producer

import (
	"s3assistant/common"
	"s3assistant/migration"
	"s3assistant/migration/broker"
)

const (
	STRING_GLUE   = "#"
	SCAN_PATTERN  = "*"
	SCAN_COUNT    = "200"
	SCAN_ORIGINAL = "original"
	SCAN_CHECK    = "check"
)

// FetchFlag used to decide how to fetch meta information,
// scan from set(stored in broker) when flag is "check" or
// fetch from s3 meta by invoking s3 list api or scanning
// original s3 meta directly.
type FetchFlag string

func (f FetchFlag) String() string {
	switch string(f) {
	case "check", "c":
		return SCAN_CHECK
	case "original", "o":
		return SCAN_ORIGINAL
	default:
		return SCAN_ORIGINAL
	}
	return ""
}

func (f FetchFlag) GetPrefix() migration.RedisPrefix {
	switch string(f) {
	case "check", "c":
		return migration.CHECK_SET_PREFIX
	case "original", "o":
		return migration.ORIGINAL_SET_PREFIX
	}
	return ""
}

type producerRedisHandler struct {
	metaRedis   *metaRedisHandler
	brokerRedis *broker.BrokerRedisHandler

	BucketName string // the bucket which producer in charge
	Cursor     string // the current scan cursor, used for producer
	Queue      string // queue used to store the information of item
	MaxKeys    string // the max keys per scan
	Flag       FetchFlag
}

func NewProducerRedisHandler(metaRedis *metaRedisHandler,
	brokerRedis *broker.BrokerRedisHandler, bucketName, queue string, flag FetchFlag) *producerRedisHandler {
	return &producerRedisHandler{
		metaRedis:   metaRedis,
		brokerRedis: brokerRedis,
		BucketName:  bucketName,
		Cursor:      common.EMPTY_STRING,
		Queue:       queue,
		MaxKeys:     SCAN_COUNT,
		Flag:        flag,
	}
}

// ValidateRedisConnection check the connectivity for redis connections
func (p *producerRedisHandler) ValidateRedisConnection() error {
	if p.metaRedis != nil {
		if err := p.metaRedis.ValidateRedisConnection(); err != nil {
			return err
		}
	}
	if p.brokerRedis != nil {
		if err := p.brokerRedis.ValidateRedisConnection(); err != nil {
			return err
		}
	}
	return nil
}

// LPUSHMigrationItem push item to broker queue
func (p *producerRedisHandler) LPUSHMigrationItem(items ...string) error {
	return p.brokerRedis.LPUSHMigrationItem(p.Queue, items...)
}

// RPOPMigrationItem pop item from broker queue
func (p *producerRedisHandler) RPOPMigrationItem() (string, error) {
	return p.brokerRedis.RPOPMigrationItem(p.Queue)
}

// SCANMigrationItems scan items form meta redis or broker redis
// which decided by flag
func (p *producerRedisHandler) SCANMigrationItems() ([]string, error) {
	objectSetKey := migration.MakeRedisKey(p.Flag.GetPrefix(), p.BucketName)
	if p.Flag.String() == SCAN_ORIGINAL {
		return p.metaRedis.SCANMigrationItems(objectSetKey, &p.Cursor, SCAN_PATTERN, SCAN_COUNT)
	} else {
		return p.brokerRedis.SCANMigrationItems(objectSetKey, &p.Cursor, SCAN_PATTERN, SCAN_COUNT)
	}
}

// setScanCursor set current scan cursor to broker redis
func (p *producerRedisHandler) SetScanCursorWithRetry() (error, bool) {
	stringKey := migration.MakeRedisKey(migration.SCAN_CURSOR_PREFIX, p.BucketName, p.Flag.String())
	return p.brokerRedis.SetScanCursorWithRetry(stringKey, p.Cursor)
}

// getScanCursor get last scan cursor to continue
func (p *producerRedisHandler) GetScanCursorWithRetry() (string, bool) {
	stringKey := migration.MakeRedisKey(migration.SCAN_CURSOR_PREFIX, p.BucketName, p.Flag.String())
	return p.brokerRedis.GetScanCursorWithRetry(stringKey)
}

// delScanCursor delete the scan cursor when all items have been traversed
func (p *producerRedisHandler) DelScanCursorWithRetry() (error, bool) {
	stringKey := migration.MakeRedisKey(migration.SCAN_CURSOR_PREFIX, p.BucketName, p.Flag.String())
	return p.brokerRedis.DelScanCursorWithRetry(stringKey)
}

// saddAddedMigrationItems store the items which have been added to broker waitting for processing
func (p *producerRedisHandler) SaddAddedMigrationItemsWithRetry(objectNames ...string) (error, bool) {
	setKey := migration.MakeRedisKey(migration.ADDED_SET_PREFIX, p.BucketName)
	return p.brokerRedis.SaddAddedMigrationItemsWithRetry(setKey, objectNames...)
}

// sdiffStoreAddedAddCompletedMigrationItems sdiff the added and completed set
// to check if there exists failed items
func (p *producerRedisHandler) SdiffStoreAddedAddCompletedMigrationItems() (int64, bool) {
	addedSetKey := migration.MakeRedisKey(migration.ADDED_SET_PREFIX, p.BucketName)
	completedSetKey := migration.MakeRedisKey(migration.COMPLETED_SET_PREFIX, p.BucketName)
	sdiffStoreSetKey := migration.MakeRedisKey(migration.CHECK_SET_PREFIX, p.BucketName)
	return p.brokerRedis.SdiffStoreAddedAndCompletedMigrationItemsWithRetry(
		sdiffStoreSetKey, addedSetKey, completedSetKey)
}

func (p *producerRedisHandler) LlenCurrentQueueLength() (int64, bool) {
	return p.brokerRedis.LLenQueueWithRetry(p.Queue)
}
