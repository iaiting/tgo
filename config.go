package tgo

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

//获取配置文件优先级 mount_configs > configs
func configGet(name string, data interface{}, defaultData interface{}) {
	absPath := getConfigPath(name)
	file, err := os.Open(absPath)
	if err != nil {
		UtilLogError(fmt.Sprintf("open %s config file failed:%s", name, err.Error()))
		data = defaultData
	} else {
		defer file.Close()
		decoder := json.NewDecoder(file)
		errDecode := decoder.Decode(data)
		if errDecode != nil {
			//记录日志
			UtilLogError(fmt.Sprintf("decode %s config error:%s", name, errDecode.Error()))
			data = defaultData
		}
	}
}

func getConfigPath(name string) (absPath string) {
	var (
		path string
		err  error
	)
	path = fmt.Sprintf("mount_configs/%s.json", name)
	_, err = os.Stat(path)
	if err != nil && os.IsNotExist(err) {
		absPath, _ = filepath.Abs(fmt.Sprintf("configs/%s.json", name))
	} else {
		absPath, _ = filepath.Abs(fmt.Sprintf("mount_configs/%s.json", name))
	}
	return
}

func configPathExist(name string) bool {
	var (
		path string
		err  error
	)
	path = fmt.Sprintf("mount_configs/%s.json", name)
	_, err = os.Stat(path)
	if err != nil && os.IsNotExist(err) {
		path = fmt.Sprintf("configs/%s.json", name)
	} else {
		return true
	}
	_, err = os.Stat(path)
	if err != nil && os.IsNotExist(err) {
		return false
	}
	return true
}

func ConfigReload() {
	configAppClear()
	configCacheClear()
	configCodeClear()
	configDbClear()
}
