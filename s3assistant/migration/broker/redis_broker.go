package broker

import (
	"s3assistant/common"
	"s3assistant/migration"
)

type RedisBroker struct {
	*BrokerRedisHandler
}

func NewRedisBroker(redisHandler *BrokerRedisHandler) *RedisBroker {
	return &RedisBroker{
		BrokerRedisHandler: redisHandler,
	}
}

// SendMigrationItem stores message to redis queue
func (r *RedisBroker) SendMigrationItem(queue string, m []*migration.MigrationItem) error {
	var encodeStrings []string = make([]string, len(m))
	var err error
	for i, _ := range m {
		if encodeStrings[i], err = m[i].Encode(); err != nil {
			return err
		}
	}
	if err = r.LPUSHMigrationItem(queue, encodeStrings...); err != nil {
		return err
	}
	return nil
}

// GetMigrationItem retrieves message from redis queue
func (r *RedisBroker) GetMigrationItem(queue string) (*migration.MigrationItem, error) {
	encodedString, err := r.RPOPMigrationItem(queue)
	if err != nil || encodedString == common.EMPTY_STRING {
		return nil, err
	}
	migrationItem, err := migration.DecodeMigrationItem(encodedString)
	if err != nil {
		return nil, err
	}
	return migrationItem, nil
}
