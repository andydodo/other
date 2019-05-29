package asyncdelete

import (
	"encoding/json"
	"io/ioutil"
	"net"
	"net/http"
	"sync"
	"time"

	"s3assistant/common"
	"s3assistant/util"

	"go.uber.org/zap"
)

const (
	HTTP_ERROR_RETRY_TIMES   = 10
	HTTP_ERROR_SLEEP_SECONDS = 10
)

type Location struct {
	Url       string `json:"url,omitempty"`
	PublicUrl string `json:"publicUrl,omitempty"`
}

type LookupResult struct {
	VolumeId  string     `json:"volumeId,omitempty"`
	Locations []Location `json:"locations,omitempty"`
	Error     string     `json:"error,omitempty"`
}

type SwClient struct {
	httpClient   *http.Client
	logger       *zap.Logger
	masterAddrs  []string
	cachedMaster string
	cacheLock    sync.RWMutex
}

func NewSwClient(logger *zap.Logger, httpClient *http.Client, addrs []string) *SwClient {
	return &SwClient{
		httpClient:   httpClient,
		masterAddrs:  append([]string{}, addrs...),
		cachedMaster: common.EMPTY_STRING,
	}
}

func (s *SwClient) RequestVolumeURL(fid string) (string, error) {
	var lastErr error
	s.cacheLock.RLock()
	cachedMaster := s.cachedMaster
	s.cacheLock.RUnlock()
	if s.cachedMaster != common.EMPTY_STRING {
		res, err := s.GetVolumeURL(cachedMaster, fid)
		if err != nil {
			lastErr = err
			s.logger.Warn("RequestVolumeURL from cached master failed",
				zap.String("fid", fid),
				zap.String("error", err.Error()),
				zap.String("requestAddr", cachedMaster))
		}
		if res != common.EMPTY_STRING {
			return res, nil
		}
	}
	for _, addr := range s.masterAddrs {
		res, err := s.GetVolumeURL(addr, fid)
		if err != nil {
			lastErr = err
			s.logger.Warn("RequestVolumeURL from other addressed failed",
				zap.String("fid", fid),
				zap.String("error", err.Error()),
				zap.String("requestAddr", addr))
		}
		if res != common.EMPTY_STRING {
			s.cacheLock.Lock()
			s.cachedMaster = addr
			s.cacheLock.Unlock()
			return res, nil
		}
	}
	return common.EMPTY_STRING, lastErr
}

func (s *SwClient) GetVolumeURL(addr, fid string) (string, error) {
	// {"VolumeId":"3","Locations":[{"Url":"xxx.xxx.xxx.xxx:xxxx","PublicUrl":"xxx.xxx.xxx.xxx:xxxx"}]}
	volumeUrl := common.EMPTY_STRING
	requestURL := "http://" + addr + "/dir/lookup?volumeId=" + fid
	resp, err := s.httpClient.Get(requestURL)
	if err != nil {
		return volumeUrl, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return volumeUrl, &util.HttpError{
			URI:      requestURL,
			HttpCode: resp.StatusCode,
			Function: "GET",
			Message:  "RequestVolumeURL failed",
		}
	}

	var lookupResult LookupResult
	if err := json.NewDecoder(resp.Body).Decode(&lookupResult); err != nil {
		return volumeUrl, err
	}
	if lookupResult.Error != common.EMPTY_STRING {
		return volumeUrl, &util.HttpError{
			URI:      requestURL,
			HttpCode: resp.StatusCode,
			Function: "GET",
			Message:  lookupResult.Error,
		}
	}
	for _, location := range lookupResult.Locations {
		if location.Url != common.EMPTY_STRING {
			volumeUrl = location.Url
			break
		}
	}
	return volumeUrl, nil
}

func (s *SwClient) DelFile(fid, volumeUrl string) error {
	uri := "http://" + volumeUrl + "/" + fid
	req, err := http.NewRequest("DELETE", uri, nil)
	if err != nil {
		return err
	}
	resp, err := s.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	switch resp.StatusCode {
	case http.StatusNotFound, http.StatusAccepted, http.StatusOK:
		return nil
	}
	m := make(map[string]interface{})
	if e := json.Unmarshal(body, m); e == nil {
		if message, ok := m["error"].(string); ok {
			return &util.HttpError{
				URI:      uri,
				HttpCode: resp.StatusCode,
				Function: "DELETE",
				Message:  message,
			}
		}
	}
	return &util.HttpError{
		URI:      uri,
		HttpCode: resp.StatusCode,
		Function: "DELETE",
		Message:  string(body),
	}
}

func (s *SwClient) requestVolumeURLWithRetry(fid string) (string, bool) {
	var requestRetry int = NET_ERROR_RETRY_TIMES
	var volumeUrl string
	var err error
	for requestRetry > 0 {
		volumeUrl, err = s.RequestVolumeURL(fid)
		if err == nil {
			return volumeUrl, true
		}
		switch err.(type) {
		case net.Error:
			requestRetry -= 1
			s.logger.Warn("RequestVolumeURL net error occurred, retry",
				zap.String("fid", fid),
				zap.String("error", err.Error()))
			time.Sleep(time.Second * time.Duration((NET_ERROR_RETRY_TIMES-requestRetry)*NET_ERROR_SLEEP_SECONDS))
			break
		case *util.HttpError:
			if err.(*util.HttpError).HttpCode == 404 {
				return common.EMPTY_STRING, true
			}
			s.logger.Warn("RequestVolumeURL http error occurred, retry",
				zap.String("fid", fid),
				zap.Int("httpCode", err.(*util.HttpError).HttpCode),
				zap.String("error", err.Error()))
			requestRetry -= 1
			time.Sleep(time.Second * time.Duration((HTTP_ERROR_RETRY_TIMES-requestRetry)*HTTP_ERROR_SLEEP_SECONDS))
			break
		default:
			return common.EMPTY_STRING, false
		}
	}
	return NET_ERROR_STRING_RETURN, true
}

// the first return value represents if operation process successfully or not,
// and the second return value indicates if program should keep doing or not.
func (s *SwClient) delFileWithRetry(fid, volumeUrl string) (bool, bool) {
	var delRetry int = NET_ERROR_RETRY_TIMES
	for delRetry > 0 {
		err := s.DelFile(fid, volumeUrl)
		if err == nil {
			return true, true
		}
		switch err.(type) {
		case net.Error:
			s.logger.Warn("Delfile net error occurred, retry",
				zap.String("fid", fid),
				zap.String("volumeURL", volumeUrl),
				zap.String("error", err.Error()))
			delRetry -= 1
			time.Sleep(time.Second * time.Duration((NET_ERROR_RETRY_TIMES-delRetry)*NET_ERROR_SLEEP_SECONDS))
		case *util.HttpError:
			if err.(*util.HttpError).HttpCode == 404 {
				return true, true
			}
			s.logger.Warn("Delfile http error occurred, retry",
				zap.String("fid", fid),
				zap.String("volumeURL", volumeUrl),
				zap.Int("httpCode", err.(*util.HttpError).HttpCode),
				zap.String("error", err.Error()))
			delRetry -= 1
			time.Sleep(time.Second * time.Duration((HTTP_ERROR_RETRY_TIMES-delRetry)*HTTP_ERROR_SLEEP_SECONDS))
			break
		default:
			return false, false
		}
	}
	return false, true
}
