package producer

import (
	"s3assistant/common"
	"s3assistant/migration/broker"
	"s3assistant/s3entry"
)

type producerStatus string

const (
	P_NONE        producerStatus = "NONE"
	P_INITIALIZED                = "INITIALIZED"
	P_PROCESSING                 = "PROCESSING"
	P_STOPPED                    = "STOPPED"
)

// ProducerInterface a common interface for all producers
type ProducerInterface interface {
	GetMetaInformation() ([]string, error)
	StartProduce()
	StopProduce()
	Init() error
	GetStatus() producerStatus
	GetQueueInfo() interface{}
}

type ProducerConfInterface interface {
	// Redis producer special fields
	GetSourceMetaConf() *common.RedisPoolConfBase

	// Pika producer special fields
	GetSourceS3Conf() *s3entry.S3Conf
	GetBrokerConf() *broker.BrokerConf
	GetQueue() string

	// Comman fields
	GetLogDir() string
	GetLogLevel() int
	GetSourceBucket() string
	GetDestinationBucket() string
	GetFetchFlag() FetchFlag
	GetServerPort() string
}

type PikaProducerConf struct {
	// Read from yaml config file
	SrcS3Conf  *s3entry.S3Conf    `yaml:"src_s3_conf"`
	Broker     *broker.BrokerConf `yaml:"broker"`
	Queue      string             `yaml:"queue"`
	LogDir     string             `yaml:"log_dir"`
	LogLevel   int                `yaml:"log_level"`
	ServerPort string             `yaml:"server_port"`
	// Fetch from command argvs
	SrcBucket string
	DstBucket string
	Flag      FetchFlag
}

func (p *PikaProducerConf) GetSourceMetaConf() *common.RedisPoolConfBase {
	return nil
}

func (p *PikaProducerConf) GetSourceS3Conf() *s3entry.S3Conf {
	return p.SrcS3Conf
}

func (p *PikaProducerConf) GetBrokerConf() *broker.BrokerConf {
	return p.Broker
}

func (p *PikaProducerConf) GetServerPort() string {
	return p.ServerPort
}

func (p *PikaProducerConf) GetQueue() string {
	return p.Queue
}

func (p *PikaProducerConf) GetLogDir() string {
	return p.LogDir
}

func (p *PikaProducerConf) GetLogLevel() int {
	return p.LogLevel
}

func (p *PikaProducerConf) GetSourceBucket() string {
	return p.SrcBucket
}

func (p *PikaProducerConf) GetDestinationBucket() string {
	return p.DstBucket
}

func (p *PikaProducerConf) GetFetchFlag() FetchFlag {
	return p.Flag
}

type RedisProducerConf struct {
	// Read from yaml config file
	SrcMetaConf *common.RedisPoolConfBase `yaml:"src_meta_conf"`
	Broker      *broker.BrokerConf        `yaml:"broker"`
	Queue       string                    `yaml:"queue"`
	LogDir      string                    `yaml:"log_dir"`
	LogLevel    int                       `yaml:"log_level"`
	ServerPort  string                    `yaml:"server_port"`
	// Fetch from command argvs
	SrcBucket string
	DstBucket string
	Flag      FetchFlag
}

func (r *RedisProducerConf) GetSourceMetaConf() *common.RedisPoolConfBase {
	return r.SrcMetaConf
}

func (r *RedisProducerConf) GetSourceS3Conf() *s3entry.S3Conf {
	return nil
}

func (r *RedisProducerConf) GetBrokerConf() *broker.BrokerConf {
	return r.Broker
}

func (r *RedisProducerConf) GetServerPort() string {
	return r.ServerPort
}

func (r *RedisProducerConf) GetQueue() string {
	return r.Queue
}

func (r *RedisProducerConf) GetLogDir() string {
	return r.LogDir
}

func (r *RedisProducerConf) GetLogLevel() int {
	return r.LogLevel
}

func (r *RedisProducerConf) GetSourceBucket() string {
	return r.SrcBucket
}

func (r *RedisProducerConf) GetDestinationBucket() string {
	return r.DstBucket
}

func (r *RedisProducerConf) GetFetchFlag() FetchFlag {
	return r.Flag
}
