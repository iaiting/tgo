package tgo

import (
	"encoding/json"
	"net/http"
	"github.com/tonyjt/gokids"
	"github.com/gin-gonic/gin"
	"time"
)

func UtilResponseReturnJsonNoP(c *gin.Context, code int, model interface{}) {
	msg := ConfigCodeGetMessage(code)
	UtilResponseReturnJsonWithMsg(c, code, msg, model, false)
}

func UtilResponseReturnJson(c *gin.Context, code int, model interface{}) {
	msg := ConfigCodeGetMessage(code)
	UtilResponseReturnJsonWithMsg(c, code, msg, model, true)
}

func UtilResponseReturnJsonWithMsg(c *gin.Context, code int, msg string, model interface{}, callbackFlag bool) {
	var (
		rj interface{}
		result bool
		callback string
	)
	if code == 0 {
		code = 1001
	}
	//添加结果
	if code == 1001 {
		result = true
	}
	startTime, _ := c.Get(gokids.KMonitorStartTimeKey)
	go monitorReport(c.Request.URL.Path, startTime, result)

	rj = gin.H{
		"code":    code,
		"message": msg,
		"data":    model,
	}

	if callbackFlag {
		callback = c.Query("callback")
	}

	if UtilIsEmpty(callback) {
		c.JSON(200, rj)
	} else {
		b, err := json.Marshal(rj)
		if err != nil {
			UtilLogErrorf("jsonp marshal error:%s", err.Error())
		} else {
			c.String(200, "%s(%s)", callback, string(b))
		}
	}
}

func UtilResponseReturnJsonFailed(c *gin.Context, code int) {
	UtilResponseReturnJson(c, code, nil)
}

func UtilResponseReturnJsonSuccess(c *gin.Context, data interface{}) {
	UtilResponseReturnJson(c, 0, data)
}

func UtilResponseRedirect(c *gin.Context, url string) {
	c.Redirect(http.StatusMovedPermanently, url)
}

func monitorReport(key string, startTime interface{}, result bool) {
	endTime := time.Now()
	if startTime == nil {
		UtilLogError("kmonitor start time is not exists")
	} else {
		startT := startTime.(time.Time)
		subTime := endTime.Sub(startT)
		err := gokids.KMonitorReport(key, "", result, subTime.Nanoseconds() / 1000000)
		if err != nil {
			UtilLogErrorf("kmonitor report error:%v", err.Error())
		}
	}
}