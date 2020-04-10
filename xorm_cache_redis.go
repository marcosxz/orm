package kit

import (
	"bytes"
	"encoding/gob"
	"hash/crc32"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-redis/redis"
)

type xOrmRedisCache struct {
	client     redis.UniversalClient
	expiration time.Duration
	bufferPool sync.Pool
}

// 使用切片查询时请定义成[]struct{}而不要定义成[]*struct{}类型！！！
func NewXOrmRedisCache(hosts []string, password string, db int, expiration time.Duration) *xOrmRedisCache {
	client := redis.NewUniversalClient(&redis.UniversalOptions{Addrs: hosts, Password: password, DB: db})
	if err := client.Ping().Err(); err != nil {
		panic(err)
	}
	return &xOrmRedisCache{client: client, expiration: expiration, bufferPool: sync.Pool{New: func() interface{} {
		return new(bytes.Buffer)
	}}}
}

func (c *xOrmRedisCache) buffer() *bytes.Buffer {
	return c.bufferPool.Get().(*bytes.Buffer)
}

func (c *xOrmRedisCache) revoke(buffer *bytes.Buffer) {
	buffer.Reset()
	c.bufferPool.Put(buffer)
}

func (c *xOrmRedisCache) exists(key string) (bool, error) {
	rows, err := c.client.Exists(key).Result()
	if err != nil {
		return false, err
	}
	if rows <= 0 {
		return false, nil
	}
	return true, nil
}

func (c *xOrmRedisCache) beanKey(tableName, id string) string {
	if id == "*" {
		return "xorm:bean:" + tableName + ":*"
	}
	crc := crc32.ChecksumIEEE([]byte(id))
	return "xorm:bean:" + tableName + ":" + strconv.Itoa(int(crc))
}

func (c *xOrmRedisCache) sqlKey(tableName, sql string) string {
	if sql == "*" {
		return "xorm:sql:" + tableName + ":*"
	}
	crc := crc32.ChecksumIEEE([]byte(sql))
	return "xorm:sql:" + tableName + ":" + strconv.Itoa(int(crc))
}

func (c *xOrmRedisCache) get(key string) (interface{}, error) {
	bs, err := c.client.Get(key).Bytes()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return c.deserialize(bs)
}

func (c *xOrmRedisCache) put(key string, value interface{}) error {
	return c.invoke(key, value)
}

func (c *xOrmRedisCache) del(key ...string) error {
	err := c.client.Del(key...).Err()
	if err != nil && !strings.Contains(err.Error(), "no such key") {
		return err
	}
	return nil
}

func (c *xOrmRedisCache) delAll(key string) error {
	var cursor uint64
	var err error
	var keys []string
next:
	var scanKeys []string
	scanKeys, cursor, err = c.client.Scan(cursor, key, 1000).Result()
	if err == redis.Nil {
		return nil
	}
	if err != nil {
		return err
	}
	keys = append(keys, scanKeys...)
	if cursor == 0 {
		if len(keys) > 0 {
			return c.del(keys...)
		}
		return nil
	}
	goto next
}

func (c *xOrmRedisCache) invoke(key string, value interface{}) error {
	bs, err := c.serialize(value)
	if err != nil {
		return err
	}
	return c.client.Set(key, bs, c.expiration).Err()
}

func (c *xOrmRedisCache) serialize(value interface{}) ([]byte, error) {
	buffer := c.buffer()
	if err := gob.NewEncoder(buffer).Encode(&value); err != nil {
		c.revoke(buffer)
		return nil, err
	}
	result := buffer.Bytes()
	c.revoke(buffer)
	return result, nil
}

func (c *xOrmRedisCache) deserialize(byt []byte) (ptr interface{}, err error) {
	buffer := c.buffer()
	buffer.Write(byt)
	if err = gob.NewDecoder(buffer).Decode(&ptr); err != nil {
		c.revoke(buffer)
		return nil, err
	}
	c.revoke(buffer)
	return ptr, nil
}

func (c *xOrmRedisCache) GetIds(tableName, sql string) interface{} {
	i, err := c.get(c.sqlKey(tableName, sql))
	if err != nil {
		log.Printf("[ERROR] Xorm Redis Cacher <GetIds> Error:%s", err.Error())
		return nil
	}
	return i
}

func (c *xOrmRedisCache) GetBean(tableName string, id string) interface{} {
	i, err := c.get(c.beanKey(tableName, id))
	if err != nil {
		log.Printf("[ERROR] Xorm Redis Cacher <GetBean> Error:%s \n", err.Error())
		return nil
	}
	return i
}

func (c *xOrmRedisCache) PutIds(tableName, sql string, ids interface{}) {
	if err := c.put(c.sqlKey(tableName, sql), ids); err != nil {
		log.Printf("[ERROR] Xorm Redis Cacher <PutIds> Error:%s \n", err.Error())
	}
}

func (c *xOrmRedisCache) PutBean(tableName string, id string, obj interface{}) {
	if err := c.put(c.beanKey(tableName, id), obj); err != nil {
		log.Printf("[ERROR] Xorm Redis Cacher <PutBean> Error:%s \n", err.Error())
	}
}

func (c *xOrmRedisCache) DelIds(tableName, sql string) {
	log.Println("[Info] Xorm Redis Cacher <DelIds>", tableName, sql)
	if err := c.del(c.sqlKey(tableName, sql)); err != nil {
		log.Printf("[ERROR] Xorm Redis Cacher <DelIds> Error:%s \n", err.Error())
	}
}

func (c *xOrmRedisCache) DelBean(tableName string, id string) {
	if err := c.del(c.beanKey(tableName, id)); err != nil {
		log.Printf("[ERROR] Xorm Redis Cacher <DelBean> Error:%s \n", err.Error())
	}
}

func (c *xOrmRedisCache) ClearIds(tableName string) {
	if err := c.delAll(c.sqlKey(tableName, "*")); err != nil {
		log.Printf("[ERROR] Xorm Redis Cacher <ClearIds> Error:%s \n", err.Error())
	}
}

func (c *xOrmRedisCache) ClearBeans(tableName string) {
	if err := c.delAll(c.beanKey(tableName, "*")); err != nil {
		log.Printf("[ERROR] Xorm Redis Cacher <ClearBeans> Error:%s \n", err.Error())
	}
}
