package tgo

import (
	"encoding/json"
	"net/http"
	"bytes"
	"github.com/gin-gonic/gin"
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
	var rj interface{}
	if code == 0 {
		code = 1001
	}
	//添加结果
	if code == 1001 {
		c.Set("result", true)
	} else {
		c.Set("result", false)
	}
	rj = gin.H{
		"code":    code,
		"message": msg,
		"data":    model,
	}

	var callback string
	if callbackFlag {
		callback = c.Query("callback")
	}

	if UtilIsEmpty(callback) {
		c.JSON(200, rj)
	} else {
		b, err := utilResponseJSONMarshal(rj)
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

func utilResponseJSONMarshal(t interface{}) ([]byte, error) {
	buffer := &bytes.Buffer{}
	encoder := json.NewEncoder(buffer)
	encoder.SetEscapeHTML(false)
	err := encoder.Encode(t)
	return buffer.Bytes(), err
}