package consumer

import (
	"net/http"
	"os"
	"s3assistant/common"
	"s3assistant/migration"
	"s3assistant/migration/broker"
	"s3assistant/util"
	"time"

	"github.com/gorilla/mux"
)

const (
	HTTP_KEEP_ALIVE_PERIOD = 20
	HTTP_IDLE_CONN_TIMEOUT = 30
)

type ConsumerHandler struct {
	HandlerStatus   common.HandlerStatus
	shutdownChannel chan struct{}

	workerConf    *WorkerConf
	workerManager *WorkerManager
}

func NewConsumerHandler(workerConf *WorkerConf) (*ConsumerHandler, error) {
	r := mux.NewRouter()
	handler, err := generateConsumer(workerConf)
	if err != nil {
		return handler, err
	}
	registerHandleFunc(r, handler)
	httpListener, err := util.NewListener("0.0.0.0:" + workerConf.ServerPort)
	if err != nil {
		return handler, err
	}
	httpService := &http.Server{Handler: r}
	go httpService.Serve(httpListener)
	return handler, nil
}

func generateConsumer(workerConf *WorkerConf) (*ConsumerHandler, error) {
	// 1. Create logger used for migration
	if err := migration.CreateLogger(workerConf.LogDir,
		"consumer", workerConf.LogLevel); err != nil {
		return nil, err
	}
	// Create temp dir
	if err := os.MkdirAll(workerConf.TempDir, os.ModePerm); err != nil {
		return nil, err
	}
	// 2. Create broker
	brokerRedisPoolConf := common.NewRedisPoolConf(
		workerConf.BrokerConf.RedisPoolConfBase,
		workerConf.WorkerNums,
		workerConf.WorkerNums)
	brokerRedisPool := common.NewRedisPool(brokerRedisPoolConf)
	brokerRedis := broker.NewBrokerRedisHandler(common.NewRedisBaseHandler(brokerRedisPool))
	// 3. Create worker redis handler
	workerRedisHandler := NewWorkerRedisHandler(brokerRedis, workerConf.Queue)
	// 4. Create http client
	httpClient := common.NewHttpClient(
		workerConf.WorkerNums,
		HTTP_KEEP_ALIVE_PERIOD,
		HTTP_IDLE_CONN_TIMEOUT)
	brokerHandler := broker.NewRedisBroker(brokerRedis)
	workerManager := NewWorkerManager(
		workerRedisHandler,
		brokerHandler,
		workerConf,
		httpClient)
	return &ConsumerHandler{
		HandlerStatus: common.HANDLER_INITIAL,
		workerManager: workerManager,
		workerConf:    workerConf,
	}, nil
}

func (c *ConsumerHandler) Init() bool {
	if !c.workerManager.Init() {
		return false
	}

	c.startRecycleGoroutine()
	c.shutdownChannel = make(chan struct{}, 1)
	c.HandlerStatus = common.HANDLER_INITIAL
	return true
}

func (c *ConsumerHandler) startRecycleGoroutine() {
	// Start goroutine to recycle resource
	go func() {
		defer func() {
			c.HandlerStatus = common.HANDLER_EXITED
			migration.Logger.Error("migration consumer handler exit successfully.")
			migration.Logger.Sync()
		}()
		for {
			select {
			case <-c.shutdownChannel:
				c.exit()
				return
			default:
				if c.workerManager.GetStatus() == WORKER_EXITED {
					return
				}
				time.Sleep(time.Second * migration.WAIT_SLEEP_SECONDS)
			}
		}
	}()
}

func (c *ConsumerHandler) Exit() {
	c.shutdownChannel <- struct{}{}
}

func (c *ConsumerHandler) Status() common.HandlerStatus {
	return c.HandlerStatus
}

func (c *ConsumerHandler) Start() {
	c.workerManager.StartWorker()
}

func (c *ConsumerHandler) exit() {
	c.workerManager.Exit()
}
