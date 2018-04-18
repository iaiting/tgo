package tgo

import (
	"io/ioutil"
	"net/http"
	"strings"
	"encoding/json"
)

func getDynamicRedisAddress(url string) (err error, address []string) {
	header := []string{"Accept:"}
	var ret []byte
	ret, err = curlGet(url, header)
	if err != nil {
		UtilLogErrorf("getDynamicRedisAddress curlGet error url: %s, err: %v", url, err)
		return
	}
	data := new(redisAddressResp)
	err = json.Unmarshal(ret, data)
	if err != nil {
		UtilLogErrorf("getDynamicRedisAddress json.Unmarshal error url: %s, ret: %s, err: %v", url, ret, err)
		return
	}
	if data.Success {
		return err, data.Content.Result
	} else {
		UtilLogErrorf("getDynamicRedisAddress result is error url: %s, data: %v", url, data)
	}
	return
}

type redisAddressResp struct {
	Success bool                    `json:"success"`
	Code    string                  `json:"code"`
	Msg     string                  `json:"msg"`
	Content redisAddressRespContent `json:"content"`
}

type redisAddressRespContent struct {
	Result []string `json:"result"`
}

func curlGet(url string, header []string) (ret []byte, err error) {
	client := &http.Client{}
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return ret, err
	}
	for _, v := range header {
		t := strings.Split(v, ":")
		length := len(t)
		if length == 2 {
			req.Header.Add(t[0], t[1])
		} else if length == 1 {
			req.Header.Add(t[0], "")
		}
	}

	resp, err := client.Do(req)
	if err != nil {
		return ret, err
	}
	defer resp.Body.Close()
	ret, err = ioutil.ReadAll(resp.Body)

	return ret, err
}
