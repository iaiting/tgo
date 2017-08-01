package tgo

import (
	"fmt"
	"github.com/jinzhu/gorm"
	"golang.org/x/net/context"
)

func MakeMysqlConnection(isRead bool, selector int) (*MysqlConnection, error) {
	var (
		dbConfig *ConfigDbBase
	)
	mysqlConfig := ConfigMysqlClusterGetOne(selector)
	if isRead {
		dbConfig = mysqlConfig.GetRead()
	} else {
		dbConfig = mysqlConfig.GetWrite()
	}
	address := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4,utf8&parseTime=True&loc=Local", dbConfig.User, dbConfig.Password, dbConfig.Address, dbConfig.Port, dbConfig.DbName)
	resultDb, err := gorm.Open("mysql", address)
	if err != nil {
		UtilLogErrorf("connect mysql error: %s", err.Error())
		return nil, err
	}

	resultDb.SingularTable(true)
	if ConfigEnvIsDev() {
		resultDb.LogMode(true)
	}

	return &MysqlConnection{resultDb, isRead}, err
}

func (p *MysqlConnectionPool) GetMysqlConnectionFromPool(isRead bool, selector int) (MysqlConnection, error) {
	ctx := context.TODO()
	r, err := p.ResourcePool.Get(ctx)
	if err != nil {
		UtilLogErrorf("connect mysql pool get error: %s", err.Error())
	}
	c, ok := r.(MysqlConnection)
	//判断conn是否正常
	if !ok || c.DB == nil {
		c, err := MakeMysqlConnection(isRead, selector)
		if err != nil {
			UtilLogErrorf("redo connect mysql error: %s", err.Error())
			p.Put(c) //放入失败的资源，保证下次重连
		}
	}
	return c, err
}
