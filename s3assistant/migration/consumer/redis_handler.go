package consumer

import (
	"s3assistant/migration"
	"s3assistant/migration/broker"
)

type workerRedisHandler struct {
	*broker.BrokerRedisHandler
	queue string
}

func NewWorkerRedisHandler(redisHandler *broker.BrokerRedisHandler, queue string) *workerRedisHandler {
	return &workerRedisHandler{
		BrokerRedisHandler: redisHandler,
		queue:              queue,
	}
}

// saddCompletedMigrationItems store the items which have been completed
func (w *workerRedisHandler) SaddCompletedMigrationItemsWithRetry(
	item *migration.MigrationItem) (error, bool) {
	setKey := migration.MakeRedisKey(migration.COMPLETED_SET_PREFIX, item.SrcBucketName)
	return w.BrokerRedisHandler.SaddCompletedMigrationItemsWithRetry(setKey, item)
}
