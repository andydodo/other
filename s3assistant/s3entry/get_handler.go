package s3entry

import (
	"fmt"
	"io"
	"os"
	"s3assistant/util"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	uuid "github.com/satori/go.uuid"
	"github.com/shirou/gopsutil/mem"
	"go.uber.org/zap"
)

const maxMultiUploadPartNumber = 10000

// FdHandle used to handle fd
type FdHandle struct {
	Fd       *os.File
	FileName string
}

func (f *FdHandle) Clear() {
	defer func() {
		f.Fd = nil
		f.FileName = EMPTY_STRING
	}()
	if f.Fd != nil {
		f.Fd.Close()
	}
	if f.FileName != EMPTY_STRING {
		_, err := os.Stat(f.FileName)
		if err == nil {
			os.Remove(f.FileName)
		}
	}
}

func (f *FdHandle) IsNull() bool {
	return f.Fd == nil && f.FileName == EMPTY_STRING
}

// GetObjectHandle used to downloaded object data for future upload operation
type GetObjectHandle struct {
	FdHandler       *FdHandle
	TempDir         string
	SrcBucketName   string
	ObjectName      string
	LastModified    string
	ObjectSize      uint64
	startIndex      uint64 // the start index in source object for next operation
	shouldDownload  uint64 // the real bytes should be downloaded for next operation
	downloaded      uint64 // the cumulative downloaded data
	ministSizePerOp uint64 // the minist size for one commit
	uncommittedSize uint64 // the current uncommit size, downloaded but have bot been handled
	committed       bool   // mark the downloaded data has been committed
	withVerison     bool
	srcCli          *S3Client
	logger          *zap.Logger
}

// Commit mark the last operation is success, update the required parameters for next
// operation, including the start index, the data size should be downloaded for next operation.
func (g *GetObjectHandle) Commit() {
	g.clearFd()
	g.startIndex += g.uncommittedSize
	g.downloaded += g.uncommittedSize
	g.shouldDownload = util.Min(g.ObjectSize-g.downloaded, g.ministSizePerOp)
	g.uncommittedSize = 0
	g.committed = true
}

// recover the downloaded state, when error occurred, clear current fd.
func (g *GetObjectHandle) recover() {
	// clear current uncommitted data, reset start index and shouldDownload size
	if g.uncommittedSize != 0 && !g.committed {
		g.clearFd()
		g.shouldDownload = util.Min(g.ObjectSize-g.downloaded, g.ministSizePerOp)
		g.uncommittedSize = 0
		g.committed = true
	}
}

func (g *GetObjectHandle) createTempFile() error {
	fileNameUniqueComponent, err := uuid.NewV4()
	if err != nil {
		return err
	}
	// Maybe wrong when the file name is the same with another item
	g.FdHandler.FileName = g.TempDir + util.Md5Str(g.SrcBucketName+g.ObjectName) +
		fileNameUniqueComponent.String() + ".temp"
	_, err = os.Stat(g.FdHandler.FileName)
	if err == nil {
		os.Remove(g.FdHandler.FileName)
	}
	g.FdHandler.Fd, err = os.OpenFile(g.FdHandler.FileName, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	return nil
}

// IsNull indicates current handler if is empty
func (g *GetObjectHandle) isNull() bool {
	return g.FdHandler.IsNull()
}

// IsComplete indicates current handler has completed downloadeded
func (g *GetObjectHandle) IsComplete() bool {
	return g.downloaded == g.ObjectSize
}

func (g *GetObjectHandle) clearFd() {
	g.FdHandler.Clear()
}

func (g *GetObjectHandle) reset() {
	g.clearFd()
	g.TempDir = EMPTY_STRING
	g.SrcBucketName = EMPTY_STRING
	g.ObjectName = EMPTY_STRING
	g.LastModified = EMPTY_STRING
	g.ObjectSize = 0
	g.startIndex = 0
	g.shouldDownload = 0
	g.downloaded = 0
	g.uncommittedSize = 0
	g.committed = true
	g.withVerison = false
	g.srcCli = nil
	g.logger = nil
}

func (g *GetObjectHandle) downloadCore() (err error) {
	// Check if there are uncommitted bytes, if yes, then return
	if g.shouldDownload == 0 || !g.committed {
		return nil
	}
	if g.isNull() {
		if err = g.createTempFile(); err != nil {
			return err
		}
	}
	defer func() {
		// Recover resources
		if err != nil {
			g.clearFd()
		}
	}()
	oldshouldDownload := g.shouldDownload
	shouldDownload := g.shouldDownload
	startIndex := g.startIndex
	// After the first download, the data size we should download
	// for this operation may be changed if object's size is empty,
	// so we record it and try a later
	if err = g.doDownload(shouldDownload, &startIndex); err != nil {
		return
	}
	if oldshouldDownload < g.shouldDownload {
		// reach here means we should download more bytes to satisfy the minist
		// part size when invoke multipartupload api
		shouldDownload = g.shouldDownload - oldshouldDownload
		if err = g.doDownload(shouldDownload, &startIndex); err != nil {
			return
		}
	}
	// mark uncommit size
	g.uncommittedSize += g.shouldDownload
	g.committed = false
	return nil
}

func (g *GetObjectHandle) doDownload(shouldDownload uint64, startIndex *uint64) (err error) {
	var objectChanged = false
	for shouldDownload > 0 {
		virtualMem, _ := mem.VirtualMemory()
		fetchBytes := util.Min(shouldDownload, uint64(virtualMem.Available/2))
		if g.withVerison {
			objectChanged, err = g.handleGetObjectRangeResult(
				g.srcCli.GetObjectRangeWithVersionId(
					g.SrcBucketName, g.ObjectName, g.LastModified, *startIndex, fetchBytes))
		} else {
			objectChanged, err = g.handleGetObjectRangeResult(
				g.srcCli.GetObjectRangeWithoutVersionId(
					g.SrcBucketName, g.ObjectName, *startIndex, fetchBytes))
		}
		if err != nil {
			return err
		}
		// Judge if object have been modified or deleted, drop current operation if yes.
		// Note: we can not handle the issue when object changed within one second.
		if objectChanged == true {
			return &util.S3Error{
				Bucket:   g.SrcBucketName,
				Key:      g.ObjectName,
				Function: OBJECT_CHANGED_FUNCTION,
			}
		}
		*startIndex += fetchBytes
		shouldDownload -= fetchBytes
	}
	return nil
}

// handleGetObjectRangeResult handle the get operation result,
// return true if object has changed.
func (g *GetObjectHandle) handleGetObjectRangeResult(
	resp *s3.GetObjectOutput, err error) (bool, error) {
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			g.logger.Error("GetObject awsError occurred",
				zap.String("Bucket", g.SrcBucketName),
				zap.String("Key", g.ObjectName),
				zap.String("errorCode", awsErr.Code()),
				zap.String("errorMessage", awsErr.Message()))
			switch awsErr.Code() {
			// Maybe have been deleted, drop it.
			case s3.ErrCodeNoSuchKey:
				return true, nil
			}
			if reqErr, ok := err.(awserr.RequestFailure); ok {
				// Maybe have been modified, drop it
				if reqErr.StatusCode() == 416 ||
					reqErr.StatusCode() == 412 ||
					reqErr.StatusCode() == 409 ||
					reqErr.StatusCode() == 404 {
					return true, nil
				}
			}
		} else {
			g.logger.Error("GetObject unexpected error occurred", zap.String("error", err.Error()))
		}
		return false, err
	}
	_, err = io.Copy(g.FdHandler.Fd, resp.Body)
	// Close to re-use tcp connection
	defer resp.Body.Close()
	if err != nil {
		return false, err
	}
	// Judgement will be failed if object changed within one second.
	lastModified := strconv.FormatUint(uint64(resp.LastModified.Unix()), 10)
	if g.LastModified == EMPTY_STRING {
		g.LastModified = lastModified
	} else if g.LastModified != lastModified {
		return true, nil
	}
	// Get object size to decide how to upload
	if g.ObjectSize == 0 {
		slices := strings.Split(*resp.ContentRange, "/")
		if len(slices) <= 1 {
			return false, &util.ParseError{
				Message: fmt.Sprintf("Parse Object Size failed, Bucket:%s, Object:%s.",
					g.SrcBucketName, g.ObjectName)}
		}
		g.ObjectSize, err = strconv.ParseUint(slices[1], 10, 64)
		if err != nil {
			return false, err
		}
		// update ministSizePerOp according object'size
		g.ministSizePerOp = util.Max(g.ObjectSize/maxMultiUploadPartNumber, g.ministSizePerOp)
		g.shouldDownload = g.ministSizePerOp
	}
	return false, nil
}

func (g *GetObjectHandle) GetObjectWithRetry() error {
	getObjectRetry := S3_RETRY_TIMES
	var err error
	for getObjectRetry > 0 {
		err = g.downloadCore()
		if err != nil {
			// Important, recorver current state
			g.recover()
			switch err.(type) {
			case *util.S3Error:
				g.logger.Warn("Object has changed, drop current migration item")
				return &util.S3Error{
					Bucket:   g.SrcBucketName,
					Key:      g.ObjectName,
					Function: OBJECT_CHANGED_FUNCTION,
				}
			default:
				g.logger.Error("GetObject error occurred, retry",
					zap.String("error", err.Error()))
				getObjectRetry -= 1
				time.Sleep(time.Second * time.Duration(
					(S3_RETRY_TIMES-getObjectRetry)*S3_RETRY_SLEEP_SECONDS))
			}
			continue
		}
		g.logger.Info("GetObject successfully")
		return nil
	}
	return err
}

var GetObjectHandlePool = sync.Pool{
	New: func() interface{} {
		return &GetObjectHandle{
			FdHandler: &FdHandle{
				Fd:       nil,
				FileName: EMPTY_STRING,
			},
			TempDir:         EMPTY_STRING,
			SrcBucketName:   EMPTY_STRING,
			ObjectName:      EMPTY_STRING,
			LastModified:    EMPTY_STRING,
			ObjectSize:      0,
			startIndex:      0,
			downloaded:      0,
			shouldDownload:  0,
			uncommittedSize: 0,
			committed:       true,
			srcCli:          nil,
			logger:          nil,
			withVerison:     false,
		}
	},
}

func FetchGetObjectHandle(tempDir, srcBucketName, objectName string, objectSize uint64,
	partSize uint64, srcCli *S3Client, logger *zap.Logger, withVerison bool) *GetObjectHandle {
	getObjectHandler := GetObjectHandlePool.Get().(*GetObjectHandle)
	getObjectHandler.TempDir = tempDir
	getObjectHandler.SrcBucketName = srcBucketName
	getObjectHandler.ObjectName = objectName
	getObjectHandler.ObjectSize = objectSize
	getObjectHandler.ministSizePerOp = util.Max(objectSize/maxMultiUploadPartNumber, partSize)
	getObjectHandler.shouldDownload = getObjectHandler.ministSizePerOp
	getObjectHandler.srcCli = srcCli
	getObjectHandler.logger = logger
	getObjectHandler.withVerison = withVerison
	return getObjectHandler
}

func RestoreGetObjectHandle(g *GetObjectHandle) {
	g.reset()
	GetObjectHandlePool.Put(g)
}
