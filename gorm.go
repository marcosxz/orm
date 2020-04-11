package orm

import (
	"errors"
	"github.com/jinzhu/gorm"
	"io"
	"log"
	"sync"
	"time"
)

var gOrmDB sync.Map

type gOrm struct {
	db         *gorm.DB
	redisCache GOrmRedisCache
}

type gOrmOptions struct {
	name             string
	driver           string // like "mysql"
	dataSource       string // like "root:root@tcp(127.0.0.1:3306)/test?charset=utf8"
	logger           *gOrmLogger
	showSQL          bool
	maxIdleConn      int
	maxOpenConn      int
	connMaxLifetime  time.Duration
	autoMigrate      []interface{}
	redisCache       *gOrmRedisCache
	redisCachePlugin GOrmRedisCache
}

type GOrmOptions func(*gOrmOptions)

type gOrmLogger struct {
	logger *log.Logger
}

func (l *gOrmLogger) Print(args ...interface{})   { l.Println(args...) }
func (l *gOrmLogger) Println(args ...interface{}) { l.logger.Println(gorm.LogFormatter(args...)...) }

func GOrmName(name string) GOrmOptions {
	return func(options *gOrmOptions) {
		options.name = name
	}
}

func GOrmDriver(driver string) GOrmOptions {
	return func(options *gOrmOptions) {
		options.driver = driver
	}
}

func GOrmDataSource(dataSource string) GOrmOptions {
	return func(options *gOrmOptions) {
		options.dataSource = dataSource
	}
}

func GOrmLogger(logger io.Writer) GOrmOptions {
	return func(options *gOrmOptions) {
		options.logger = &gOrmLogger{
			logger: log.New(logger, "", 0),
		}
	}
}

func GOrmShowSQL(showSQL bool) GOrmOptions {
	return func(options *gOrmOptions) {
		options.showSQL = showSQL
	}
}

func GOrmMaxIdleConn(maxIdleConn int) GOrmOptions {
	return func(options *gOrmOptions) {
		options.maxIdleConn = maxIdleConn
	}
}

func GOrmMaxOpenConn(maxOpenConn int) GOrmOptions {
	return func(options *gOrmOptions) {
		options.maxOpenConn = maxOpenConn
	}
}

func GOrmConnMaxLifetime(connMaxLifetime time.Duration) GOrmOptions {
	return func(options *gOrmOptions) {
		options.connMaxLifetime = connMaxLifetime
	}
}

func GOrmAutoMigrate(autoMigrate ...interface{}) GOrmOptions {
	return func(options *gOrmOptions) {
		options.autoMigrate = autoMigrate
	}
}

func GOrmOpenRedisCache(hosts []string, password string, db int, expiration time.Duration) GOrmOptions {
	return func(options *gOrmOptions) {
		options.redisCache = NewGOrmRedisCache(hosts, password, db, expiration)
	}
}

func initGOrmOptions(options ...GOrmOptions) (*gOrmOptions, error) {
	opts := &gOrmOptions{}
	for _, opt := range options {
		opt(opts)
	}
	if opts.name == "" {
		return nil, errors.New("gorm name is empty")
	}
	return opts, nil
}

func InitGOrmDB(options ...GOrmOptions) error {
	opts, err := initGOrmOptions(options...)
	if err != nil {
		return err
	}
	db, err := gorm.Open(opts.driver, opts.dataSource)
	if err != nil {
		return err
	}
	if opts.logger != nil {
		db.SetLogger(opts.logger)
	}
	if opts.maxOpenConn > 0 {
		db.DB().SetMaxOpenConns(opts.maxOpenConn)
	}
	if opts.maxIdleConn > 0 {
		db.DB().SetMaxIdleConns(opts.maxIdleConn)
	}
	if opts.connMaxLifetime > 0 {
		db.DB().SetConnMaxLifetime(opts.connMaxLifetime)
	}
	if opts.showSQL {
		if err = db.LogMode(opts.showSQL).Error; err != nil {
			return err
		}
	}
	if len(opts.autoMigrate) > 0 {
		if err = db.AutoMigrate(opts.autoMigrate...).Error; err != nil {
			return err
		}
	}
	if opts.redisCache != nil {
		opts.redisCachePlugin = opts.redisCache.SetCacheDB(db)
		if opts.showSQL {
			opts.redisCachePlugin.Debug()
		}
	}
	gOrmRegister(opts.name, db, opts.redisCachePlugin)
	return nil
}

func gOrmRegister(name string, db *gorm.DB, plugin GOrmRedisCache) {
	gOrmDB.Store(name, &gOrm{db: db, redisCache: plugin})
}

func GOrmDB(name string) *gorm.DB {
	if db, ok := gOrmDB.Load(name); ok && db != nil {
		return db.(*gOrm).db
	}
	return nil
}

func GOrmRedisCacheDB(name string) GOrmRedisCache {
	if db, ok := gOrmDB.Load(name); ok && db != nil {
		return db.(*gOrm).redisCache
	}
	return nil
}
