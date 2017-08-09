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
	//fmt.Println("dao_mysql_cluster")
	if ConfigMysqlClusterGetDbCount() > 0 {
		configMysqlClusterInit()
		poolTicker := time.NewTicker(time.Second * 60)
		MysqlClusterReadPool = make(map[int]*MysqlConnectionPool)
		MysqlClusterWritePool = make(map[int]*MysqlConnectionPool)

		for selector, mysqlConfig := range mysqlClusterConfig.MysqlCluster {
			//fmt.Printf("db:%d\n", selector)
			initMysqlClusterPool(true, selector)
			go monitorPool(mysqlConfig.GetPool(), poolTicker, true, MysqlClusterReadPool[selector])

			initMysqlClusterPool(false, selector)
			go monitorPool(mysqlConfig.GetPool(), poolTicker, false, MysqlClusterWritePool[selector])
		}
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
			//fmt.Println("reader")
			//fmt.Printf("selector:%d\n", selector)
			mysqlClusterReadPoolMux.Lock()
			defer mysqlClusterReadPoolMux.Unlock()

			MysqlClusterReadPool[selector] = NewMysqlConnectionPool(mysqlConnectionFactory, configPool.PoolMinCap,
				configPool.PoolMaxCap, configPool.PoolIdleTimeout*time.Millisecond)
		}

		//fmt.Printf("MysqlClusterReadPool:%+v\n", MysqlClusterReadPool[selector].ResourcePool)
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
	//fmt.Println(orm)
	//return nil
	errInsert := orm.Table(d.TableName).Create(model).Error
	if errInsert != nil {
		//记录
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
		//记录
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
		//记录
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
		//记录
		UtilLogError(fmt.Sprintf("table:%s count error:%s, condition:%s", d.TableName, errInsert.Error(), condition))
	}

	return errInsert
}
