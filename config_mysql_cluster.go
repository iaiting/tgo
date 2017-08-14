package tgo

import (
	"math/rand"
	"sync"
	"time"
)

var (
	mysqlClusterConfig    *ConfigMysqlCluster
	mysqlClusterConfigMux sync.Mutex
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
		configFileName := "mysql_cluster"
		if configPathExist(configFileName) {
			mysqlClusterConfigMux.Lock()
			defer mysqlClusterConfigMux.Unlock()
			mysqlClusterConfig = NewConfigMysqlCluster()
			defaultMysqlClusterConfig := configMysqlClusterGetDefault()
			configGet(configFileName, mysqlClusterConfig, defaultMysqlClusterConfig)
		}
	}
}

func (m *ConfigMysql) GetClusterPool() *ConfigDbPool {
	return &m.Pool
}

func (m *ConfigMysql) GetClusterWrite() (config *ConfigDbBase) {
	config = &m.Write
	config.DbName = m.DbName
	return
}

func (m *ConfigMysql) GetClusterRead() (config *ConfigDbBase) {
	readConfigs := m.Reads
	count := len(readConfigs)
	i := 0
	if count > 1 {
		rand.Seed(time.Now().UnixNano())
		i = rand.Intn(count - 1)
	}
	config = &readConfigs[i]
	config.DbName = m.DbName

	return
}
