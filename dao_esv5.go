package tgo

import (
	"context"
	"github.com/jolestar/go-commons-pool"
	"gopkg.in/olivere/elastic.v5"
	"net/http"
	"sync"
	"time"
)

type DaoESV5 struct {
	IndexName string
	TypeName  string
}

var (
	esv5Pool      *pool.ObjectPool
	esv5PoolMux   sync.Mutex
	esv5Client    *elastic.Client
	esv5ClientMux sync.Mutex
)

func getESV5PoolConfig() *pool.ObjectPoolConfig {
	config := configESGet()

	return &pool.ObjectPoolConfig{
		Lifo:               config.ClientLifo,
		BlockWhenExhausted: true,
		MaxWaitMillis:      config.ClientMaxWaitMillis,
		MaxIdle:            config.ClientMaxIdle,
		MaxTotal:           config.ClientMaxTotal,
		TestOnBorrow:       true,
		MinIdle:            config.ClientMinIdle}
}

func (dao *DaoESV5) GetConnect() (*elastic.Client, error) {

	config := configESGet()

	if esv5Client == nil {
		esv5ClientMux.Lock()
		defer esv5ClientMux.Unlock()

		if esv5Client == nil {
			clientHttp := &http.Client{
				Transport: &http.Transport{
					MaxIdleConnsPerHost: config.TransportMaxIdel,
				},
				Timeout: time.Duration(config.Timeout) * time.Millisecond,
			}
			client, err := elastic.NewClient(elastic.SetHttpClient(clientHttp), elastic.SetURL(config.Address...))
			if err != nil {
				// Handle error

				UtilLogErrorf("es connect error :%s,address:%v", err.Error(), config)

				return nil, err
			}
			esv5Client = client
		}

	}

	return esv5Client, nil
}

func (dao *DaoESV5) CloseConnect(client *elastic.Client) {
	//esPool.ReturnObject(client)
}

func (dao *DaoESV5) Insert(id string, data interface{}) error {
	client, err := dao.GetConnect()

	if err != nil {
		return err
	}
	defer dao.CloseConnect(client)

	ctx := context.Background()
	_, errRes := client.Index().Index(dao.IndexName).Type(dao.TypeName).Id(id).BodyJson(data).Do(ctx)

	if errRes != nil {
		UtilLogErrorf("insert error :%s", errRes.Error())
		return errRes
	}

	return nil
}

func (dao *DaoESV5) Update(id string, doc interface{}) error {
	client, err := dao.GetConnect()

	if err != nil {
		return err
	}
	defer dao.CloseConnect(client)
	ctx := context.Background()
	_, errRes := client.Update().Index(dao.IndexName).Type(dao.TypeName).Id(id).
		Doc(doc).
		Do(ctx)

	if errRes != nil {
		UtilLogErrorf("DaoESV5 Update error :%s", errRes.Error())
		return err
	}

	return nil
}

func (dao *DaoESV5) UpdateAppend(id string, name string, value interface{}) error {
	client, err := dao.GetConnect()

	if err != nil {
		return err
	}

	ctx := context.Background()
	_, errRes := client.Update().Index(dao.IndexName).Type(dao.TypeName).Id(id).
		Script(elastic.NewScriptFile("append-reply").Param("reply", value)).
		Do(ctx)

	if errRes != nil {
		UtilLogErrorf("DaoESV5 Update error :%s", errRes.Error())
		return err
	}

	return nil
}
