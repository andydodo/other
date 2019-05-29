package producer

import (
	"net/http"
	"s3assistant/common"
	"s3assistant/migration"
	"s3assistant/migration/broker"
	"s3assistant/s3entry"
	"s3assistant/util"
	"time"

	"github.com/gorilla/mux"
	"go.uber.org/zap"
)

type ProducerHandler struct {
	HandlerStatus   common.HandlerStatus
	ShutdownChannel chan struct{} // Passive shutdown triggered by caller

	producerConf ProducerConfInterface
	producer     ProducerInterface
}

func NewProducerHandler(producerConf ProducerConfInterface) (*ProducerHandler, error) {
	//	r := mux.NewRouter()
	handler, err := generateProducer(producerConf)
	if err != nil {
		return handler, err
	}
	//	registerHandleFunc(r, handler)
	//	httpListener, err := util.NewListener("0.0.0.0:" + producerConf.GetServerPort())
	//	if err != nil {
	//		return handler, err
	//	}
	//	httpService := &http.Server{Handler: r}
	//	go httpService.Serve(httpListener)
	return handler, nil
}

func registerHandleFunc(r *mux.Router, p *ProducerHandler) {
	r.HandleFunc("/producer/status", p.statusHandler)
}

func (p *ProducerHandler) statusHandler(w http.ResponseWriter, r *http.Request) {
	m := make(map[string]interface{})
	m["Bucket"] = p.producerConf.GetSourceBucket()
	m["Queue"] = p.currentQueueInfo()
	common.WriteJson(w, r, http.StatusOK, m)
}

func (p *ProducerHandler) currentQueueInfo() interface{} {
	m := make(map[string]interface{})
	m["Name"] = p.producerConf.GetQueue()
	m["Length"] = p.queueInfo()
	return m
}

func (p *ProducerHandler) Init() bool {
	if err := p.producer.Init(); err != nil {
		migration.Logger.Error("Initialize producer failed",
			zap.String("errorInfo", err.Error()))
		return false
	}

	p.ShutdownChannel = make(chan struct{}, 1)
	p.HandlerStatus = common.HANDLER_INITIAL
	p.startRecycleGoroutine()
	return true
}

func (p *ProducerHandler) startRecycleGoroutine() {
	// Start goroutine to recycle resource
	go func() {
		defer func() {
			p.HandlerStatus = common.HANDLER_EXITED
			migration.Logger.Error("migration producer handler exit successfully.")
			migration.Logger.Sync()
		}()
		for {
			select {
			case <-p.ShutdownChannel:
				p.Exit()
				return
			default:
				if p.producer.GetStatus() == P_STOPPED {
					return
				}
				time.Sleep(time.Second * migration.WAIT_SLEEP_SECONDS)
				continue
			}
		}
	}()
}

func (p *ProducerHandler) queueInfo() interface{} {
	return p.producer.GetQueueInfo()
}

func (p *ProducerHandler) Exit() {
	p.producer.StopProduce()
}

func (p *ProducerHandler) Status() common.HandlerStatus {
	return p.HandlerStatus
}

func (p *ProducerHandler) Start() {
	p.producer.StartProduce()
}

func generateProducer(producerConf ProducerConfInterface) (*ProducerHandler, error) {
	// 1. Create logger used for migration
	if err := migration.CreateLogger(producerConf.GetLogDir(),
		"producer", producerConf.GetLogLevel()); err != nil {
		return nil, err
	}
	// Create broker redis connection pool
	if producerConf.GetBrokerConf() == nil {
		return nil, &util.HandlerError{
			Handler:   "migration producer",
			ErrorInfo: "broker config empty",
		}
	}
	brokerRedisPoolConf := common.NewRedisPoolConf(producerConf.GetBrokerConf().RedisPoolConfBase, 10, 10)
	brokerRedisPool := common.NewRedisPool(brokerRedisPoolConf)
	brokerRedis := broker.NewBrokerRedisHandler(common.NewRedisBaseHandler(brokerRedisPool))
	// Create meta redis connection pool
	var metaRedis *metaRedisHandler = nil
	if producerConf.GetSourceMetaConf() != nil {
		metaRedisPoolConf := common.NewRedisPoolConf(producerConf.GetSourceMetaConf(), 10, 10)
		metaRedisPool := common.NewRedisPool(metaRedisPoolConf)
		metaRedis = NewMetaRedisHandler(common.NewRedisBaseHandler(metaRedisPool))
	}
	// Create producer redis handler
	producerRedisHandler := NewProducerRedisHandler(
		metaRedis,
		brokerRedis,
		producerConf.GetSourceBucket(),
		producerConf.GetQueue(),
		producerConf.GetFetchFlag())
	// Create broker instance
	brokerHandler := broker.NewRedisBroker(brokerRedis)
	// Create base producer
	baseProducer := NewProducer(
		producerRedisHandler,
		brokerHandler,
		producerConf.GetSourceBucket(),
		producerConf.GetDestinationBucket(),
		producerConf.GetQueue())

	// Create s3 client
	var s3Client *s3entry.S3Client = nil
	if producerConf.GetSourceS3Conf() != nil {
		s3Client = s3entry.NewS3Client(
			migration.Logger,
			producerConf.GetSourceS3Conf(),
			http.DefaultClient)
	}
	// Create http server
	switch producerConf.(type) {
	case *PikaProducerConf:
		return &ProducerHandler{
			producer:      NewPikaProducer(baseProducer, s3Client),
			producerConf:  producerConf.(*PikaProducerConf),
			HandlerStatus: common.HANDLER_NONE,
		}, nil
	case *RedisProducerConf:
		return &ProducerHandler{
			producer:      NewRedisProducer(baseProducer),
			producerConf:  producerConf.(*RedisProducerConf),
			HandlerStatus: common.HANDLER_NONE,
		}, nil
	default:
		return nil, &util.HandlerError{
			Handler:   "migration producer",
			ErrorInfo: "unkown producer type",
		}
	}
}
