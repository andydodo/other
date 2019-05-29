package consumer

import (
	"net/http"
	"s3assistant/common"

	"github.com/gorilla/mux"
)

func registerHandleFunc(r *mux.Router, c *ConsumerHandler) {
	r.HandleFunc("/consumer/status", c.statusHandler)
	r.HandleFunc("/consumer/update", c.updateHandler)
}

func (c *ConsumerHandler) statusHandler(w http.ResponseWriter, r *http.Request) {
	m := make(map[string]interface{})
	m["Worker"] = c.workerStatus()
	common.WriteJson(w, r, http.StatusOK, m)
}

func (c *ConsumerHandler) updateHandler(w http.ResponseWriter, r *http.Request) {
	m := make(map[string]interface{})
	consumerQueue := r.FormValue("queue")
	if consumerQueue != "" {
		c.setQueue(consumerQueue)
		m["Queue"] = consumerQueue
		common.WriteJson(w, r, http.StatusOK, m)
		return
	}
	m["Status"] = "unkown"
	common.WriteJson(w, r, http.StatusNotFound, m)
	return
}

func (c *ConsumerHandler) workerStatus() interface{} {
	m := make(map[string]interface{})
	m["Numbers"] = c.workerConf.WorkerNums
	m["Active"] = c.workerManager.getActiveWorkerNums()
	return m
}

func (c *ConsumerHandler) setQueue(queue string) {
	c.workerManager.setConsumerQueue(queue)
}
