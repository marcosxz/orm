package orm

import (
	"fmt"
	"github.com/go-redis/redis"
	"github.com/jinzhu/gorm"
	"log"
	"time"
)

type GOrmTimeSharding interface {
	OrgName() string
	Sharding() string
}

type GOrmDBTimeSharding struct {
	db          *gorm.DB
	tables      map[string]GOrmTimeSharding
	records     map[string]bool
	redisClient *redis.Client
	lockTimeout time.Duration
}

func NewGOrmDBTimeSharding(db *gorm.DB) *GOrmDBTimeSharding {
	s := &GOrmDBTimeSharding{
		db:      db,
		tables:  make(map[string]GOrmTimeSharding),
		records: make(map[string]bool),
	}
	go s.startAutoShardingTimer()
	return s
}

func (s *GOrmDBTimeSharding) RedisLock(client *redis.Client, timeout time.Duration) *GOrmDBTimeSharding {
	s.redisClient = client
	s.lockTimeout = timeout
	return s
}

func (s *GOrmDBTimeSharding) Table(t GOrmTimeSharding) *GOrmDBTimeSharding {
	s.tables[t.OrgName()] = t
	if err := s.createSharding(t); err != nil {
		panic(err)
	}
	return s
}

func (s *GOrmDBTimeSharding) TableName(t GOrmTimeSharding) string {
	return TableName(t)
}

func TableName(t GOrmTimeSharding) string {
	tableName := t.OrgName()
	if t.Sharding() != "" {
		tableName += "_" + time.Now().Format(t.Sharding())
	}
	return tableName
}

func (s *GOrmDBTimeSharding) createSharding(t GOrmTimeSharding) (err error) {
	if s.redisClient != nil { // 加分布式锁
		var ok bool
		if ok, err = s.lock(t, s.lockTimeout); err != nil {
			return err
		} else if !ok { // 其他人已经持有锁
			return nil
		} else { // 我拿到锁,任务完成后释放锁
			defer func() { err = s.unlock(t) }()
		}
	}
	if err = s.autoMigrate(t, time.Now()); err != nil { // 创建当前表
		return
	}
	switch t.Sharding() {
	case "2006010215": // 按小时分表
		return s.autoMigrate(t, time.Now().Add(time.Hour))
	case "20060102": // 按天分表
		return s.autoMigrate(t, time.Now().AddDate(0, 0, 1))
	case "200601": // 按月分表
		return s.autoMigrate(t, time.Now().AddDate(0, 1, 0))
	case "2006": // 按年分表
		return s.autoMigrate(t, time.Now().AddDate(1, 0, 0))
	case "": // 不分表
		return nil
	default:
		return fmt.Errorf("not support sharding format:%s", t.Sharding())
	}
}

func (s *GOrmDBTimeSharding) autoMigrate(t GOrmTimeSharding, shardingTime time.Time) (err error) {
	tableName := t.OrgName()
	if t.Sharding() != "" {
		tableName += "_" + shardingTime.Format(t.Sharding())
	}
	if !s.records[tableName] {
		if s.db.HasTable(tableName) {
			s.records[tableName] = true
		} else {
			if err = s.db.Table(tableName).AutoMigrate(t).Error; err == nil {
				s.records[tableName] = true
			}
		}
	}
	return
}

func (s *GOrmDBTimeSharding) startAutoShardingTimer() {
	timer := time.NewTimer(time.Hour)
	for {
		select {
		case <-timer.C:
			for _, sharding := range s.tables {
				if err := s.createSharding(sharding); err != nil {
					log.Printf("[ERROR] gorm time sharding auto timer create table '%s' error: %v", sharding.OrgName(), err)
				}
			}
			timer.Reset(time.Hour)
		}
	}
}

func (s *GOrmDBTimeSharding) unlock(t GOrmTimeSharding) error {
	_, err := s.redisClient.Del("gorm:timesharding:" + t.OrgName()).Result()
	return err
}

func (s *GOrmDBTimeSharding) lock(t GOrmTimeSharding, timeout time.Duration) (ok bool, err error) {
	return s.redisClient.SetNX("gorm:timesharding:"+t.OrgName(), t.Sharding(), timeout).Result()
}
