package common

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"time"
)

func NewHttpClient(workerNums int, keepAlivePeriod, idleConnTimeout int64) *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			DialContext: (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: time.Duration(keepAlivePeriod) * time.Second,
			}).DialContext,
			MaxIdleConnsPerHost:   workerNums,
			MaxIdleConns:          20 * workerNums,
			IdleConnTimeout:       time.Duration(idleConnTimeout) * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
		},
	}
}

func WriteJson(w http.ResponseWriter, r *http.Request, httpStatus int, obj interface{}) (err error) {
	var bytes []byte
	if r.FormValue("pretty") != "" {
		bytes, err = json.MarshalIndent(obj, "", "  ")
	} else {
		bytes, err = json.Marshal(obj)
	}
	if err != nil {
		return
	}
	callback := r.FormValue("callback")
	if callback == "" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(httpStatus)
		_, err = w.Write(bytes)
	} else {
		w.Header().Set("Content-Type", "application/javascript")
		w.WriteHeader(httpStatus)
		if _, err = w.Write([]uint8(callback)); err != nil {
			return
		}
		if _, err = w.Write([]uint8("(")); err != nil {
			return
		}
		fmt.Fprint(w, string(bytes))
		if _, err = w.Write([]uint8(")")); err != nil {
			return
		}
	}
	return
}
