package tgo

import (
	"sync"
)

var (
	mysqlClusterConfig    *ConfigMysqlCluster
	mysqlClusterConfigMux *sync.Mutex
)

type ConfigMysqlCluster struct {
	MysqlCluster []*ConfigMysql
}

func NewConfigMysqlCluster() *ConfigMysqlCluster {
	return &ConfigMysqlCluster{}
}

func ConfigMysqlClusterGetAll() []*ConfigMysql {
	configMysqlClusterInit()
	return mysqlClusterConfig.MysqlCluster
}

func ConfigMysqlClusterGetOne(selector int) *ConfigMysql {
	configMysqlClusterInit()
	configMysql := NewConfigMysql()
	if selector <= ConfigMysqlClusterGetDbCount() {
		configMysql = mysqlClusterConfig.MysqlCluster[selector]
	}
	return configMysql
}

func ConfigMysqlClusterGetDbCount() int {
	configMysqlClusterInit()
	if mysqlClusterConfig == nil || len(mysqlClusterConfig.MysqlCluster) == 0 || mysqlClusterConfig.MysqlCluster[0].DbName == "" {
		return 0
	}
	return len(mysqlClusterConfig.MysqlCluster)
}

func configMysqlClusterGetDefault() *ConfigMysqlCluster {
	configMysql := &ConfigMysql{
		DbName: "",
		Pool:   ConfigDbPool{5, 5, 20, 3600, 100, 60},
		Write:  ConfigDbBase{"ip", 33062, "user", "password", ""},
		Reads: []ConfigDbBase{ConfigDbBase{"ip", 3306, "user", "password", ""},
			ConfigDbBase{"ip", 33062, "user", "password", ""}}}
	return &ConfigMysqlCluster{MysqlCluster: []*ConfigMysql{configMysql}}
}

func configMysqlClusterInit() {
	if mysqlClusterConfig == nil || len(mysqlClusterConfig.MysqlCluster) == 0 || mysqlClusterConfig.MysqlCluster[0].DbName == "" {
		mysqlClusterConfigMux.Lock()
		defer mysqlClusterConfigMux.Unlock()
		mysqlClusterConfig = NewConfigMysqlCluster()
		defaultMysqlClusterConfig := configMysqlClusterGetDefault()
		configGet("mysql_cluster", mysqlClusterConfig, defaultMysqlClusterConfig)
	}
}
