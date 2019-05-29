package producer

import (
	"s3assistant/migration"
	"s3assistant/s3entry"

	"go.uber.org/zap"
)

const listKeysCount = 1000

type PikaProducer struct {
	*Producer
	*s3entry.S3Client
}

func NewPikaProducer(producer *Producer,
	client *s3entry.S3Client) *PikaProducer {
	return &PikaProducer{
		Producer: producer,
		S3Client: client,
	}
}

func (p *PikaProducer) Init() error {
	return p.Producer.ValidateResource()
}

func (p *PikaProducer) GetMetaInformation() ([]string, error) {
	switch p.Producer.producerRedisHandler.Flag.String() {
	case "check":
		// Scan from check queue
		return p.Producer.producerRedisHandler.SCANMigrationItems()
	case "original":
		// List objects by s3api
		listObjectsV2Output, err := p.S3Client.ListObjectsV2(
			p.Producer.producerRedisHandler.BucketName,
			&p.Producer.producerRedisHandler.Cursor,
			listKeysCount)
		if err != nil {
			return nil, err
		}
		var result []string
		objects := listObjectsV2Output.Contents
		result = make([]string, len(objects))
		for index, object := range objects {
			result[index] = *object.Key
		}
		return result, nil
	}
	return nil, nil
}

func (p *PikaProducer) StartProduce() {
	go func() {
		defer func() {
			// log and mark exit
			migration.Logger.Error("PikaProducer exit.")
			migration.Logger.Sync()
			p.Producer.exitedChannel <- struct{}{}
		}()
		// call StartProduce to initialize
		err, flag := p.Producer.InitialProduce()
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
			case <-p.Producer.shutDownChannel:
				return
			default:
				var objectNames []string
				var err error
				// 1. Get Meta
				for i := 0; i < retry; i++ {
					objectNames, err = p.GetMetaInformation()
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
				if continueFlag := p.Producer.StartProduce(objectNames); !continueFlag {
					return
				}
			}
		}
	}()
}

func (p *PikaProducer) GetQueueInfo() interface{} {
	length, flag := p.Producer.producerRedisHandler.LlenCurrentQueueLength()
	if !flag {
		return -1
	}
	return length
}

func (p *PikaProducer) StopProduce() {
	p.Producer.StopProduce()
}

func (p *PikaProducer) GetStatus() producerStatus {
	return p.Producer.getStatus()
}
