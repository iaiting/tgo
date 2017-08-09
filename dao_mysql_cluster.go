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
	MysqlClusterReadPool = make(map[int]*MysqlConnectionPool)
	MysqlClusterWritePool = make(map[int]*MysqlConnectionPool)

	for selector, mysqlConfig := range mysqlClusterConfig.MysqlCluster {
		initMysqlClusterPool(true, selector)
		go monitorClusterPool(mysqlConfig.GetClusterPool(), poolTicker, MysqlClusterReadPool[selector])
		initMysqlClusterPool(false, selector)
		go monitorClusterPool(mysqlConfig.GetClusterPool(), poolTicker, MysqlClusterWritePool[selector])
	}
}

func monitorClusterPool(configPool *ConfigDbPool, poolTicker *time.Ticker, mysqlPool *MysqlConnectionPool) {
	var (
		caps         int
		poolCaps     int
		oldWaitCount int64
		waitCount    int64
	)
	for {
		waitCount = mysqlPool.WaitCount() - oldWaitCount
		oldWaitCount = mysqlPool.WaitCount()
		poolCaps = int(mysqlPool.Capacity())
		if waitCount >= configPool.PoolWaitCount && poolCaps != configPool.PoolMaxCap { //定时循环内超出多少等待数目
			caps = poolCaps + configPool.PoolExCap
		} else if waitCount == 0 && poolCaps != configPool.PoolMinCap { //闲时减少池子容量
			caps = poolCaps - configPool.PoolExCap
		} else {
			<-poolTicker.C
			continue
		}
		if caps < configPool.PoolMinCap {
			caps = configPool.PoolMinCap
		}
		if caps > configPool.PoolMaxCap {
			caps = configPool.PoolMaxCap
		}
		mysqlPool.SetCapacity(caps)
		<-poolTicker.C
	}
}

func initMysqlClusterPool(isRead bool, selector int) *MysqlConnectionPool {
	mysqlConfig := ConfigMysqlClusterGetOne(selector)

	configPool := mysqlConfig.GetClusterPool()
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

func (c MysqlConnection) PutCluster(d *DaoMysqlCluster) {
	if c.IsRead {
		MysqlClusterReadPool[d.DbSelector].Put(c)
	} else {
		MysqlClusterWritePool[d.DbSelector].Put(c)
	}
}

func (d *DaoMysqlCluster) Insert(model interface{}) error {
	orm, err := d.GetWriteOrm()
	if err != nil {
		return err
	}
	defer orm.PutCluster(d)
	errInsert := orm.Table(d.TableName).Create(model).Error
	if errInsert != nil {
		UtilLogError(fmt.Sprintf("insert into table:%s error:%s, data:%+v", d.TableName, errInsert.Error(), model))
	}

	return errInsert
}

func (d *DaoMysqlCluster) Update(condition string, sets map[string]interface{}) error {
	orm, err := d.GetWriteOrm()
	if err != nil {
		return err
	}
	defer orm.PutCluster(d)
	errInsert := orm.Table(d.TableName).Where(condition).Updates(sets).Error
	if errInsert != nil {
		UtilLogError(fmt.Sprintf("update table:%s error:%s, condition:%s, sets:%+v", d.TableName, errInsert.Error(), condition, sets))
	}

	return errInsert
}

func (d *DaoMysqlCluster) Remove(condition string) error {
	orm, err := d.GetWriteOrm()
	if err != nil {
		return err
	}
	defer orm.PutCluster(d)
	errInsert := orm.Table(d.TableName).Where(condition).Delete(nil).Error
	if errInsert != nil {
		UtilLogError(fmt.Sprintf("remove from table:%s error:%s, condition:%s", d.TableName, errInsert.Error(), condition))
	}

	return errInsert
}

func (d *DaoMysqlCluster) Select(condition string, data interface{}, skip int, limit int, fields []string, sort string) error {
	orm, err := d.GetReadOrm()
	if err != nil {
		return err
	}
	defer orm.PutCluster(d)
	db := orm.Table(d.TableName).Where(condition)

	if len(fields) > 0 {
		db = db.Select(fields)
	}
	if skip > 0 {
		db = db.Offset(skip)
	}
	if limit > 0 {
		db = db.Limit(limit)
	}
	if sort != "" {
		db = db.Order(sort)
	}
	errFind := db.Find(data).Error

	return errFind
}

func (d *DaoMysqlCluster) First(condition string, data interface{}) error {
	orm, err := d.GetReadOrm()
	if err != nil {
		return err
	}
	defer orm.PutCluster(d)
	db := orm.Table(d.TableName).Where(condition)
	errFind := db.First(data).Error

	return errFind
}

func (d *DaoMysqlCluster) Count(condition string, data interface{}) error {
	orm, err := d.GetReadOrm()
	if err != nil {
		return err
	}
	defer orm.PutCluster(d)

	errInsert := orm.Table(d.TableName).Where(condition).Count(data).Error
	if errInsert != nil {
		UtilLogError(fmt.Sprintf("table:%s count error:%s, condition:%s", d.TableName, errInsert.Error(), condition))
	}

	return errInsert
}
