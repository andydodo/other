package command

import (
	"fmt"
	"os"
	"strings"
	"time"

	"s3assistant/common"
	"s3assistant/migration/consumer"
	"s3assistant/migration/producer"
	"s3assistant/util"
)

var (
	m MigrationOptions
)

var producerUsage = "migration -role=producer -src=bucket1 -dst=bucket2 -type=redis -flag=original -config=./producer.yaml"
var consumerUsage = "s3assistant migration -role=consumer -config=./consumer.yaml"
var cmdMigration = &Command{
	UsageLine: producerUsage + "\nor\t" + consumerUsage,
	Short:     "migrate all objects from source bucket to destination bucket",
	Long:      "",
}

var migrationRole = []string{
	"producer",
	"consumer",
}

var producerType = []string{
	"redis",
	"pika",
}

var fetchFlag = []string{
	"check",
	"original",
}

type MigrationOptions struct {
	role         *string
	producerType *string
	srcBucket    *string
	dstBucket    *string
	fetchFlag    *string
	configFile   *string
}

func init() {
	m.role = cmdMigration.Flag.String("role", "", "Migration role (can only be producer or consumer)")
	m.producerType = cmdMigration.Flag.String("type", "redis", "[producer] producer type (pika or redis), redis is default")
	m.fetchFlag = cmdMigration.Flag.String("flag", "original", "[producer] producer start model (original or check), origin is default")
	m.srcBucket = cmdMigration.Flag.String("src", "", "[producer] Migration source bucket")
	m.dstBucket = cmdMigration.Flag.String("dst", "", "[producer] Migration destination bucket")
	m.configFile = cmdMigration.Flag.String("config", "", "[producer & consumer] config file path")
	cmdMigration.Run = runMigration
}

func runMigration(stop chan struct{}) bool {
	// 1. Check parameters
	var validation bool = false
	for i := range migrationRole {
		if migrationRole[i] == *m.role {
			validation = true
			break
		}
	}
	if !validation {
		return false
	}
	for i := range producerType {
		if producerType[i] == *m.producerType {
			validation = true
			break
		}
	}
	if !validation {
		return false
	}
	for i := range fetchFlag {
		if fetchFlag[i] == *m.fetchFlag {
			validation = true
			break
		}
	}
	if !validation {
		return false
	}
	// 2. create handler entry
	handlerEntry, err := m.CreateCommandEntry()
	if err != nil {
		fmt.Fprintf(os.Stderr, "create command entry failed %s", err.Error())
		return false
	}
	// 3. Initialize and run
	if !handlerEntry.Init() {
		fmt.Fprintf(os.Stderr, "init command entry failed")
		return false
	}
	cmdMigration.Status = CommandInit
	handlerEntry.Start()
	cmdMigration.Status = CommandProcessing

	// 4. Start goroutine to handle signal
	go func() {
		defer func() {
			cmdMigration.Status = CommandExited
		}()
		for {
			select {
			case <-stop:
				handlerEntry.Exit()
				continue
			default:
				if handlerEntry.Status() == common.HANDLER_EXITED {
					return
				}
				time.Sleep(time.Second * DefaultSleepTime)
				continue
			}
		}
	}()
	return true
}

func (m *MigrationOptions) CreateCommandEntry() (common.HandlerEntry, error) {
	switch *m.role {
	case "producer":
		if strings.ToLower(*m.producerType) == "redis" {
			return m.createRedisProducerHandler()
		}
		if strings.ToLower(*m.producerType) == "pika" {
			return m.createPikaProducerHandler()
		}
		return nil, nil
	case "consumer":
		return m.createWorkerHandler()
	default:
		return nil, nil
	}
}

func (m *MigrationOptions) createRedisProducerHandler() (common.HandlerEntry, error) {
	var redisProducerConf producer.RedisProducerConf
	if ok, err := util.ParseConf(*m.configFile, &redisProducerConf); !ok {
		return nil, err
	}
	redisProducerConf.DstBucket = *m.dstBucket
	redisProducerConf.SrcBucket = *m.srcBucket
	redisProducerConf.Flag = producer.FetchFlag(*m.fetchFlag)
	return producer.NewProducerHandler(&redisProducerConf)
}

func (m *MigrationOptions) createPikaProducerHandler() (common.HandlerEntry, error) {
	var pikaProducerConf producer.PikaProducerConf
	if ok, err := util.ParseConf(*m.configFile, &pikaProducerConf); !ok {
		return nil, err
	}
	pikaProducerConf.DstBucket = *m.dstBucket
	pikaProducerConf.SrcBucket = *m.srcBucket
	pikaProducerConf.Flag = producer.FetchFlag(*m.fetchFlag)
	return producer.NewProducerHandler(&pikaProducerConf)
}

func (m *MigrationOptions) createWorkerHandler() (common.HandlerEntry, error) {
	var workerConf consumer.WorkerConf
	if ok, err := util.ParseConf(*m.configFile, &workerConf); !ok {
		return nil, err
	}
	return consumer.NewConsumerHandler(&workerConf)
}
