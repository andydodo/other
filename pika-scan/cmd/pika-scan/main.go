package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/http/pprof"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/andy/pika-scan/common"
	"github.com/andy/pika-scan/scan"
	"github.com/andy/pika-scan/util"
	"github.com/gorilla/mux"
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
	maxInt     = flag.Int("length", 100, "PikaScanOptions length of producer queue")
	numWorker  = flag.Int("numbers", 10, "PikaScanOptions number of consumer worker")
	port       = flag.Int("port", 9527, "PikaScanOptions port of listen on scan")
)

var usage = `Usage: pika-scan [options...] <path>

  Options:
      -bucket  which bucket need to scan 
      -config  scan pika config file path 
      -length  length of producer queue
      -numbers number of consumer worker 
      -port    port of listen on scan web server 

`

type consumer struct {
	hand *scan.ScanRedisHandler
}

func newConsumer(h *scan.ScanRedisHandler) *consumer {
	return &consumer{
		hand: h,
	}
}

type producer struct {
	channel chan string
	bucket  string
}

func newProducer(ch chan string, bucketName string) *producer {
	return &producer{
		channel: ch,
		bucket:  bucketName,
	}
}

func (c *consumer) start(ch chan string, id int) {
	for {
		select {
		case object, ok := <-ch:
			if !ok {
				log.Printf("exit groutine id: %v beacuse of channel closed \n", id)
				return
			}
			mp, err := c.hand.GETObjToFid(object)
			if err != nil {
				log.Printf("exit groutine id: %v beacuse of get object to fid: %s \n", id, err.Error())
				return
			}

			for i := len(mp.Fids) - 1; i >= 0; i-- {
				fid := strings.Trim(mp.Fids[i], "")
				if fid != "" {
					fmt.Print(mp.Object, "\t", fid, "\r\n")
				}
			}

		}
	}
}

func (p *producer) statusHandler(w http.ResponseWriter, r *http.Request) {
	m := make(map[string]interface{})
	m["Bucket"] = p.bucket
	m["Channel"] = len(p.channel)
	common.WriteJson(w, r, http.StatusOK, m)
}

func startPprof(p *producer) {
	r := mux.NewRouter()
	r.HandleFunc("/status", p.statusHandler)
	r.HandleFunc("/debug/pprof/", pprof.Index)
	r.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	r.HandleFunc("/debug/pprof/profile", pprof.Profile)
	r.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	r.HandleFunc("/debug/pprof/trace", pprof.Trace)

	if err := http.ListenAndServe(":"+strconv.Itoa(*port), r); err != nil {
		log.Fatalf("cannot start pika scan: %s", err.Error())
	}
}

func main() {
	flag.Usage = func() {
		fmt.Fprint(os.Stderr, usage)
	}

	flag.Parse()

	handler, err := CreateHandler()
	if err != nil {
		log.Println(err.Error())
		return
	}
	consumer := newConsumer(handler)
	//objList := MakeObjectList(handler.Config.Bucket)
	objList := MakeObjectList(*bucketName)
	ch := make(chan string, *maxInt)
	producer := newProducer(ch, *bucketName)
	go startPprof(producer)
	var wg sync.WaitGroup

	for i := 0; i < *numWorker; i++ {
		wg.Add(1)
		go func(ch chan string, i int) {
			defer wg.Done()
			consumer.start(ch, i)
		}(ch, i)
	}

	go func(h *scan.ScanRedisHandler, objlist string, maxint string, channel chan string) {
		defer close(channel)
		files, err := h.SCANBucketObjs(objlist, maxint)
		if err != nil {
			log.Printf("scan bucket file failed %s", err.Error())
		} else {
			for i := range files {
				channel <- files[i]
			}
		}
	}(handler, objList, strconv.Itoa(*maxInt), ch)

	wg.Wait()

}

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
