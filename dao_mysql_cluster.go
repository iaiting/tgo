package tgo

import (
	"fmt"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/youtube/vitess/go/pools"
)

var (
	MysqlClusterReadPool     map[int]*MysqlConnectionPool
	mysqlClusterReadPoolMux  sync.Mutex
	MysqlClusterWritePool    map[int]*MysqlConnectionPool
	mysqlClusterWritePoolMux sync.Mutex
)

type DaoMysqlCluster struct {
	TableName  string
	DbSelector int
}

func NewDaoMysqlCluster() *DaoMysqlCluster {
	return &DaoMysqlCluster{}
}

func initCluster() {
	configMysqlClusterInit()
	poolTicker := time.NewTicker(time.Second * 60)

	for selector, mysqlConfig := range mysqlClusterConfig.MysqlCluster {
		initMysqlClusterPool(true, selector)
		go monitorPool(mysqlConfig.GetPool(), poolTicker, true, MysqlClusterReadPool[selector])

		initMysqlClusterPool(false, selector)
		go monitorPool(mysqlConfig.GetPool(), poolTicker, false, MysqlClusterWritePool[selector])
	}
}

func initMysqlClusterPool(isRead bool, selector int) *MysqlConnectionPool {
	mysqlConfig := ConfigMysqlClusterGetOne(selector)
	configPool := mysqlConfig.GetPool()
	mysqlConnectionFactory := func() (pools.Resource, error) {
		return MakeMysqlConnection(isRead, selector)
	}
	if isRead {
		if MysqlClusterReadPool[selector] == nil || MysqlClusterReadPool[selector].IsClosed() {
			mysqlClusterReadPoolMux.Lock()
			defer mysqlClusterReadPoolMux.Unlock()

			MysqlClusterReadPool[selector] = NewMysqlConnectionPool(mysqlConnectionFactory, configPool.PoolMinCap,
				configPool.PoolMaxCap, configPool.PoolIdleTimeout*time.Millisecond)
		}
		return MysqlClusterReadPool[selector]
	} else {
		if MysqlClusterWritePool[selector] == nil || MysqlClusterWritePool[selector].IsClosed() {
			mysqlClusterWritePoolMux.Lock()
			defer mysqlClusterWritePoolMux.Unlock()

			MysqlClusterWritePool[selector] = NewMysqlConnectionPool(mysqlConnectionFactory, configPool.PoolMinCap,
				configPool.PoolMaxCap, configPool.PoolIdleTimeout*time.Millisecond)
		}
		return MysqlClusterWritePool[selector]
	}
}

func (d *DaoMysqlCluster) GetReadOrm() (MysqlConnection, error) {
	return d.getOrm(true)
}

func (d *DaoMysqlCluster) GetWriteOrm() (MysqlConnection, error) {
	return d.getOrm(false)
}

func (d *DaoMysqlCluster) getOrm(isRead bool) (MysqlConnection, error) {

	return initMysqlClusterPool(isRead, d.DbSelector).GetMysqlConnectionFromPool(isRead, d.DbSelector)
}

func (d *DaoMysqlCluster) Insert(model interface{}) error {
	orm, err := d.GetWriteOrm()
	if err != nil {
		return err
	}
	defer orm.Put()
	errInsert := orm.Table(d.TableName).Create(model).Error
	if errInsert != nil {
		//记录
		UtilLogError(fmt.Sprintf("insert data error:%s", errInsert.Error()))
	}

	return errInsert
}

func (d *DaoMysqlCluster) Select(condition string, data interface{}, field ...[]string) error {
	orm, err := d.GetReadOrm()
	if err != nil {
		return err
	}
	defer orm.Put()
	var errFind error
	if len(field) == 0 {
		errFind = orm.Table(d.TableName).Where(condition).Find(data).Error
	} else {
		errFind = orm.Table(d.TableName).Where(condition).Select(field[0]).Find(data).Error
	}

	return errFind
}
