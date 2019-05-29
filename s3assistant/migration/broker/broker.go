package broker

import (
	"s3assistant/common"
	"s3assistant/migration"
)

type BrokerConf struct {
	RedisPoolConfBase *common.RedisPoolConfBase `yaml:"redis_pool_conf"`
}

// Broker is interface for broker database
type Broker interface {
	SendMigrationItem(queue string, items []*migration.MigrationItem) error
	GetMigrationItem(queue string) (*migration.MigrationItem, error)
}
