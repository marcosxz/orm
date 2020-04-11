package orm

import (
	"github.com/8treenet/gcache"
	"github.com/8treenet/gcache/option"
	"github.com/jinzhu/gorm"
	"strings"
	"time"
)

type GOrmRedisCache interface {
	gcache.Plugin
}

type gOrmRedisCache struct {
	hosts    []string
	password string
	db       int
	options  *gcache.DefaultOption
}

func NewGOrmRedisCache(hosts []string, password string, db int, expiration time.Duration) *gOrmRedisCache {
	cache := &gOrmRedisCache{hosts: hosts, password: password, db: db}
	cache.options = &option.DefaultOption{Opt: option.Opt{
		Expires:         int(expiration / time.Second), // 缓存时间，默认120秒。范围 30-3600
		Level:           option.LevelSearch,            // 缓存级别，默认LevelSearch。LevelDisable:关闭缓存，LevelModel:模型缓存， LevelSearch:查询缓存
		AsyncWrite:      false,                         // 异步缓存更新, 默认false。 insert update delete 成功后是否异步更新缓存。 ps: affected如果未0，不触发更新。
		PenetrationSafe: false,                         // 开启防穿透, 默认false。 ps:防击穿强制全局开启。
	}}
	return cache
}

func (c *gOrmRedisCache) SetCacheDB(db *gorm.DB) GOrmRedisCache {
	return gcache.AttachDB(db, c.options, &option.RedisOption{
		Addr:     strings.Join(c.hosts, ","),
		Password: c.password,
		DB:       c.db,
	})
}
