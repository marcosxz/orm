package orm

import (
	"errors"
	"io"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/go-xorm/xorm"
	"xorm.io/core"
)

var (
	xOrmEngine       sync.Map              // make(map[string]*xorm.Engine)
	XOrmSameMapper   = core.SameMapper{}   // 驼峰
	XOrmSnakeMapper  = core.SnakeMapper{}  // 蛇形
	XOrmPrefixMapper = core.PrefixMapper{} // 前缀
	XOrmSuffixMapper = core.SuffixMapper{} // 后缀
)

// 考虑缩写
type XOrmGonicIMapper struct {
	core.GonicMapper
}

type xOrmOption struct {
	name               string
	driver             string // like "mysql"
	dataSource         string // like "root:root@tcp(127.0.0.1:3306)/test?charset=utf8"
	caches             map[string]core.Cacher
	defaultCache       core.Cacher
	disableGlobalCache bool
	logger             core.ILogger
	maxIdleConn        int
	maxOpenConn        int
	connMaxLifetime    time.Duration
	schema             string
	tZDatabase         *time.Location
	tZLocation         *time.Location
	showExecTime       bool
	showSQL            bool
	noAutoTime         bool
	bufferSize         int
	charset            string
	cascade            bool // 级联
	sync               []interface{}
	sync2              []interface{}
	mapper             core.IMapper
	tableMapper        core.IMapper
	columnMapper       core.IMapper
}

type XOrmOption func(*xOrmOption)

func XOrmEngineName(name string) XOrmOption {
	return func(option *xOrmOption) {
		option.name = name
	}
}

func XOrmDriver(driver string) XOrmOption {
	return func(option *xOrmOption) {
		option.driver = driver
	}
}

func XOrmDataSource(dataSource string) XOrmOption {
	return func(option *xOrmOption) {
		option.dataSource = dataSource
	}
}

func XOrmCache(table string, cache core.Cacher) XOrmOption {
	return func(option *xOrmOption) {
		option.caches[table] = cache
	}
}

func XOrmDefaultCache(cache core.Cacher) XOrmOption {
	return func(option *xOrmOption) {
		option.defaultCache = cache
	}
}

func XOrmDisableGlobalCache(disable bool) XOrmOption {
	return func(option *xOrmOption) {
		option.disableGlobalCache = disable
	}
}

func XOrmLogger(logger io.Writer) XOrmOption {
	return func(option *xOrmOption) {
		option.logger = xorm.NewSimpleLogger(logger)
	}
}

func XOrmMaxIdleConn(max int) XOrmOption {
	return func(option *xOrmOption) {
		option.maxIdleConn = max
	}
}

func XOrmMaxOpenConn(max int) XOrmOption {
	return func(option *xOrmOption) {
		option.maxOpenConn = max
	}
}

func XOrmConnMaxLifetime(life time.Duration) XOrmOption {
	return func(option *xOrmOption) {
		option.connMaxLifetime = life
	}
}

func XOrmSchema(schema string) XOrmOption {
	return func(option *xOrmOption) {
		option.schema = schema
	}
}

func XOrmTZDatabase(tz *time.Location) XOrmOption {
	return func(option *xOrmOption) {
		option.tZDatabase = tz
	}
}

func XOrmTZLocation(tz *time.Location) XOrmOption {
	return func(option *xOrmOption) {
		option.tZLocation = tz
	}
}

func XOrmShowExecTime(show bool) XOrmOption {
	return func(option *xOrmOption) {
		option.showExecTime = show
	}
}

func XOrmShowSQL(show bool) XOrmOption {
	return func(option *xOrmOption) {
		option.showSQL = show
	}
}

func XOrmBufferSize(size int) XOrmOption {
	return func(option *xOrmOption) {
		option.bufferSize = size
	}
}

func XOrmCharset(charset string) XOrmOption {
	return func(option *xOrmOption) {
		option.charset = charset
	}
}

func XOrmCascade(cascade bool) XOrmOption {
	return func(option *xOrmOption) {
		option.cascade = cascade
	}
}

func XOrmNoAutoTime(noAutoTime bool) XOrmOption {
	return func(option *xOrmOption) {
		option.noAutoTime = noAutoTime
	}
}

func XOrmSync(beans ...interface{}) XOrmOption {
	return func(option *xOrmOption) {
		option.sync = beans
	}
}

func XOrmSync2(beans ...interface{}) XOrmOption {
	return func(option *xOrmOption) {
		option.sync2 = beans
	}
}

func XOrmMapper(mapper core.IMapper) XOrmOption {
	return func(option *xOrmOption) {
		option.mapper = mapper
	}
}

func XOrmTableMapper(mapper core.IMapper) XOrmOption {
	return func(option *xOrmOption) {
		option.tableMapper = mapper
	}
}

func XOrmColumnMapper(mapper core.IMapper) XOrmOption {
	return func(option *xOrmOption) {
		option.columnMapper = mapper
	}
}

func NewXOrmPrefixMapper(prefix string, mapper core.IMapper) core.IMapper {
	return core.NewPrefixMapper(mapper, prefix)
}

func NewXOrmSuffixMapper(suffix string, mapper core.IMapper) core.IMapper {
	return core.NewSuffixMapper(mapper, suffix)
}

func initXOrmOptions(options ...XOrmOption) (*xOrmOption, error) {
	opts := &xOrmOption{caches: make(map[string]core.Cacher)}
	for _, opt := range options {
		opt(opts)
	}
	if opts.name == "" {
		return nil, errors.New("xorm engine name is empty")
	}
	return opts, nil
}

func InitXOrmEngine(options ...XOrmOption) error {
	opts, err := initXOrmOptions(options...)
	if err != nil {
		return err
	}
	if engine, err := xorm.NewEngine(opts.driver, opts.dataSource); err != nil {
		return err
	} else {
		if err := setXOrmEngineOptions(engine, opts); err != nil {
			return err
		}
		xOrmRegister(opts.name, engine)
		return nil
	}
}

func setXOrmEngineOptions(engine *xorm.Engine, opts *xOrmOption) error {
	for t, c := range opts.caches {
		engine.SetCacher(t, c)
	}
	if opts.defaultCache != nil {
		engine.SetDefaultCacher(opts.defaultCache)
	}
	if opts.logger != nil {
		engine.SetLogger(opts.logger)
	}
	if opts.maxIdleConn > 0 {
		engine.SetMaxIdleConns(opts.maxIdleConn)
	}
	if opts.maxOpenConn > 0 {
		engine.SetMaxOpenConns(opts.maxOpenConn)
	}
	if opts.connMaxLifetime > 0 {
		engine.SetConnMaxLifetime(opts.connMaxLifetime)
	}
	if opts.schema != "" {
		engine.SetSchema(opts.schema)
	}
	if opts.tZDatabase != nil {
		engine.SetTZDatabase(opts.tZDatabase)
	}
	if opts.tZLocation != nil {
		engine.SetTZLocation(opts.tZLocation)
	}
	if opts.bufferSize > 0 {
		engine.BufferSize(opts.bufferSize)
	}
	if opts.charset != "" {
		engine.Charset(opts.charset)
	}
	if opts.mapper != nil {
		engine.SetMapper(opts.mapper)
	}
	if opts.tableMapper != nil {
		engine.SetTableMapper(opts.mapper)
	}
	if opts.columnMapper != nil {
		engine.SetColumnMapper(opts.mapper)
	}
	engine.SetDisableGlobalCache(opts.disableGlobalCache)
	engine.ShowExecTime(opts.showExecTime)
	engine.ShowSQL(opts.showSQL)
	engine.Cascade(opts.cascade)
	if opts.noAutoTime {
		engine.NoAutoTime()
	}
	if len(opts.sync) > 0 {
		if err := engine.Sync(opts.sync...); err != nil {
			return err
		}
	}
	if len(opts.sync2) > 0 {
		if err := engine.Sync2(opts.sync2...); err != nil {
			return err
		}
	}
	return nil
}

func xOrmRegister(name string, engine *xorm.Engine) {
	xOrmEngine.Store(name, engine)
}

func XOrmEngine(name string) *xorm.Engine {
	if engine, ok := xOrmEngine.Load(name); ok && engine != nil {
		return engine.(*xorm.Engine)
	}
	return nil
}

func XOrmSession(name string) *xorm.Session {
	if e := XOrmEngine(name); e != nil {
		return e.NewSession()
	}
	return nil
}
