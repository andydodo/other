package migration

import (
	"encoding/base64"
	"encoding/json"
	"s3assistant/common"
	"strings"
	"sync"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	LLEN_QUEUE                                            = "llenQueue"
	LPUSH_MIGRATE_ITEM_FUNCTION                           = "LPUSHMigrationItem"
	RPOP_MIGRATE_ITEM_FUNCTION                            = "RPOPMigrationItem"
	SCAN_MIGRATE_ITEMS_FUNCTION                           = "SCANMigrationItems"
	DEL_SCAN_CURSOR_FUNCTION                              = "delScanCursor"
	GET_SCAN_CURSOR_FUNCTION                              = "getScanCursor"
	SET_SCAN_CURSOR_FUNCTION                              = "setScanCursor"
	SADD_ADDED_MIGRATE_ITEMS_FUNCTION                     = "saddAddedMigrationItems"
	SADD_COMPLETED_MIGRATE_ITEMS_FUNCTION                 = "saddCompletedMigrationItems"
	SDIFFSTORE_ADDED_AND_COMPLETED_MIGRATE_ITEMS_FUNCTION = "sdiffStoreAddedAddCompletedMigrationItems"
)

type RedisPrefix string

const (
	ORIGINAL_SET_PREFIX    RedisPrefix = "_ZOL_"
	CHECK_SET_PREFIX                   = "_CHECK_SET_"
	COMPLETED_SET_PREFIX               = "_COMPLETED_SET_"
	SDIFF_STORE_SET_PREFIX             = "_SDIFF_ADDED_AND_COMPLETED_"
	ADDED_SET_PREFIX                   = "_ADDED_SET_"
	SCAN_CURSOR_PREFIX                 = "_SCAN_CURSOR_"
)

const stringGlue = "#"

func MakeRedisKey(prefix RedisPrefix, args ...string) string {
	return string(prefix) + strings.Join(args, stringGlue)
}

const (
	WAIT_SLEEP_SECONDS      = 5
	NET_ERROR_SLEEP_SECONDS = 10
	NET_ERROR_RETRY_TIMES   = 3
	EMPTY_STRING            = ""
)

var Logger *zap.Logger = nil

func CreateLogger(logDir, fileName string, logLevel int) (err error) {
	Logger, err = common.LoggerWrapper.CreateLogger(logDir, fileName, logLevel)
	return
}

// MigrationItem represents the meta information used for migration operation
type MigrationItem struct {
	ObjectName    string `json:"Object"`
	SrcBucketName string `json:"SrcBucket"`
	DstBucketName string `json:"DstBucket"`
}

func (m *MigrationItem) MarshalJson() string {
	res, err := json.Marshal(m)
	if err != nil {
		Logger.Error("MigrationItem marshal error",
			zap.String("errorInfo", err.Error()))
		return EMPTY_STRING
	}
	return string(res)
}

func (m *MigrationItem) UnmarshalJSON(data string) error {
	if err := json.Unmarshal([]byte(data), m); err != nil {
		return err
	}
	return nil
}

func (m *MigrationItem) Reset() {
	m.ObjectName = EMPTY_STRING
	m.SrcBucketName = EMPTY_STRING
	m.DstBucketName = EMPTY_STRING
}

// Encode returns base64 json encoded string
func (m *MigrationItem) Encode() (string, error) {
	jsonData, err := json.Marshal(m)
	if err != nil {
		return EMPTY_STRING, err
	}
	encodedData := base64.StdEncoding.EncodeToString(jsonData)
	return encodedData, err
}

func (m *MigrationItem) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddString("srcBucketName", m.SrcBucketName)
	enc.AddString("dstBucketName", m.DstBucketName)
	enc.AddString("objectName", m.ObjectName)
	return nil
}

var MigrationItemPool = sync.Pool{
	New: func() interface{} {
		return &MigrationItem{
			ObjectName:    EMPTY_STRING,
			SrcBucketName: EMPTY_STRING,
			DstBucketName: EMPTY_STRING,
		}
	},
}

func FetchMigrationItem() *MigrationItem {
	return MigrationItemPool.Get().(*MigrationItem)
}

func RestoreMigrationItem(ms ...*MigrationItem) {
	if ms == nil {
		return
	}
	for i, _ := range ms {
		ms[i].Reset()
		MigrationItemPool.Put(ms[i])
	}
}

// DecodeMigrationItem decodes base64 encrypted body and return MigrationItem object
func DecodeMigrationItem(encodedBody string) (*MigrationItem, error) {
	body, err := base64.StdEncoding.DecodeString(encodedBody)
	if err != nil {
		return nil, err
	}
	item := FetchMigrationItem()
	err = json.Unmarshal(body, item)
	if err != nil {
		RestoreMigrationItem(item)
		return nil, err
	}
	return item, nil
}

func ParseMigrationItem(jsonBody string) (*MigrationItem, error) {
	item := FetchMigrationItem()
	if err := item.UnmarshalJSON(jsonBody); err != nil {
		RestoreMigrationItem(item)
		return nil, err
	}
	return item, nil
}
