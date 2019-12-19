package main

import (
	"flag"
	"fmt"
	"log"
	"sync"

	//"net/http"
	//"net/http/pprof"
	"os"

	"github.com/andy/pika-scan/common"
	"github.com/andy/pika-scan/scan"
	"github.com/andy/pika-scan/util"
	//"github.com/gorilla/mux"
)

const (
	OBJECT_PREFIX            = "OBJECT"
	OBJECT_PART_PREFIX       = "OBJECTPART"
	OBJECT_PART_GROUP_PREFIX = "OBJECTPART_GROUP"
	STRING_GLUE              = "#"
	SCAN_PATTERN             = "*"
)

var (
	bucketName = flag.String("bucket", "", "Which bucket need to scan")
	configFile = flag.String("config", "", "PikaScanOptions config file path")
)

var usage = `Usage: pika-scan [options...] <path>

  Options:
      -bucket which bucket need to scan 
      -config scan pika config file path 

`

type consumer struct {
	hand *scan.ScanRedisHandler
}

func newConsumer(h *scan.ScanRedisHandler) *consumer {
	return &consumer{
		hand: h,
	}
}

func (c *consumer) start(ch <-chan string, id int) {
	for {
		select {
		case object, ok := <-ch:
			if !ok {
				log.Printf("exit groutine id: %v beacuse of channel closed \n", id)
				return
			}
			mp, err := c.hand.GETObjToFid(object)
			if err != nil {
				log.Printf("exit groutine id: %v beacuse of: %s \n", id, err.Error())
				return
			}
			fmt.Println(mp)
		}
	}
}

func main() {
	flag.Usage = func() {
		fmt.Fprint(os.Stderr, usage)
	}

	flag.Parse()

	//go startPprof()
	handler, err := CreateHandler()
	if err != nil {
		log.Println(err.Error())
		return
	}
	consumer := newConsumer(handler)
	//objList := MakeObjectList(handler.Config.Bucket)
	objList := MakeObjectList(*bucketName)
	ch := make(chan string, 1000)
	var wg sync.WaitGroup

	/*
		go func(ch <-chan string) {
			for {
				object, ok := <-ch
				if !ok {
					fmt.Println("nil")
					return
				}
				mp, err := handler.GETObjToFid(object)
				if err != nil {
					log.Println(err.Error())
					return
				}
				fmt.Println(mp)
			}
		}(ch)
	*/
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(ch chan string, i int) {
			defer wg.Done()
			consumer.start(ch, i)
		}(ch, i)
	}

	_, er := handler.SCANBucketObjs(objList, "1000", ch)
	if er != nil {
		log.Println(er.Error())
	}
	/*
		for _, v := range res {
			mp, e := handler.GETObjToFid(v)
			if e != nil {
				log.Println(e.Error())
				return
			}
			fmt.Println(mp)
		}
	*/
	wg.Wait()
}

/*
func startPprof() {
	r := mux.NewRouter()
	r.HandleFunc("/debug/pprof/", pprof.Index)
	r.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	r.HandleFunc("/debug/pprof/profile", pprof.Profile)
	r.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	r.HandleFunc("/debug/pprof/trace", pprof.Trace)

	if err := http.ListenAndServe(":9527", r); err != nil {
		log.Fatalf("cannot start pika scan: %s", err)
	}
}
*/

func MakeObjectList(bucket string) string {
	return OBJECT_PREFIX + STRING_GLUE + bucket + STRING_GLUE + SCAN_PATTERN
}

func CreateHandler() (*scan.ScanRedisHandler, error) {
	var ScanConf scan.ScanConf
	if ok, err := util.ParseConf(*configFile, &ScanConf); !ok {
		return nil, err
	}

	if ScanConf.RedisPoolConfBase == nil {
		return nil, &util.RedisError{
			Message:  "RedisPoolConfBase is nil",
			Function: "nil",
		}
	}
	redisConnectionNumber := 10
	var (
		RedisPoolConf *common.RedisPoolConf = common.NewRedisPoolConf(ScanConf.RedisPoolConfBase,
			redisConnectionNumber, redisConnectionNumber)
		RedisBaseHandler *common.RedisBaseHandler = common.NewRedisBaseHandler(common.NewRedisPool(RedisPoolConf))
	)
	if err := RedisBaseHandler.ValidateRedisConnection(); err != nil {
		return nil, &util.RedisError{
			Message:   "validate original redis connection failed",
			Function:  "ping",
			ErrorInfo: err.Error(),
		}
	}

	logger, err := common.LoggerWrapper.CreateLogger(ScanConf.LogDir, "pika-scan", ScanConf.LogLevel)
	if err != nil {
		return nil, err
	}

	handler := scan.NewScanRedisHandler(logger, RedisBaseHandler, &ScanConf)

	return handler, nil
}
