package producer

import (
	"s3assistant/migration"

	"go.uber.org/zap"
)

type RedisProducer struct {
	*Producer
}

func NewRedisProducer(producer *Producer) *RedisProducer {
	return &RedisProducer{
		Producer: producer,
	}
}

func (r *RedisProducer) Init() error {
	return r.Producer.ValidateResource()
}

func (r *RedisProducer) GetMetaInformation() ([]string, error) {
	return r.Producer.producerRedisHandler.SCANMigrationItems()
}

func (r *RedisProducer) StartProduce() {
	go func() {
		defer func() {
			migration.Logger.Error("RedisProducer exit.")
			migration.Logger.Sync()
			r.Producer.exitedChannel <- struct{}{}
		}()
		err, flag := r.Producer.InitialProduce()
		if err != nil {
			migration.Logger.Error("StartProduce failed",
				zap.String("errorInfo", err.Error()))
			return
		}
		if flag {
			migration.Logger.Info("StartProduce successfully, no other items need to handle")
			return
		}
		retry := 3
		for {
			select {
			case <-r.Producer.shutDownChannel:
				return
			default:
				var objectNames []string
				var err error
				// 1. Get Meta
				for i := 0; i < retry; i++ {
					objectNames, err = r.GetMetaInformation()
					if err != nil {
						if i == retry {
							migration.Logger.Error("GetMetaInformation failed after retry",
								zap.String("errorInfo", err.Error()))
							return
						} else {
							migration.Logger.Warn("GetMetaInformation failed, we will retry",
								zap.String("errorInfo", err.Error()))
							continue
						}
					}
					break
				}
				// 2. invoke StartProduce
				if continueFlag := r.Producer.StartProduce(objectNames); !continueFlag {
					return
				}
			}
		}
	}()
}

func (r *RedisProducer) GetQueueInfo() interface{} {
	length, flag := r.Producer.producerRedisHandler.LlenCurrentQueueLength()
	if !flag {
		return -1
	}
	return length
}

func (r *RedisProducer) StopProduce() {
	r.Producer.StopProduce()
}

func (r *RedisProducer) GetStatus() producerStatus {
	return r.Producer.getStatus()
}
