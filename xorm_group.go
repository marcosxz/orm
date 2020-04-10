package kit

import (
	"errors"
	"github.com/go-xorm/xorm"
	"sync"
)

const (
	xOrmRandomPolicy = iota
	xOrmLeastConnPolicy
	xOrmRoundRobinPolicy
)

var (
	xOrmEngineGroup sync.Map // make(map[string]*xorm.EngineGroup)
)

type xOrmGroupOption struct {
	name       string
	master     *xorm.Engine
	slaves     []*xorm.Engine
	policy     map[int]xorm.GroupPolicy
	realPolicy xorm.GroupPolicy
	weight     []int
	isWeight   bool
}

type XOrmGroupOption func(*xOrmGroupOption)

func XOrmGroupName(name string) XOrmGroupOption {
	return func(option *xOrmGroupOption) {
		option.name = name
	}
}

func XOrmMaster(master *xorm.Engine) XOrmGroupOption {
	return func(option *xOrmGroupOption) {
		option.master = master
	}
}

// 如果设置权重值,则自动切换GroupPolicy为WeightPolicy
// 如果没有设置GroupPolicy,则默认使用WeightRoundRobinPolicy
func XOrmSlave(slave *xorm.Engine, weight int) XOrmGroupOption {
	return func(option *xOrmGroupOption) {
		option.slaves = append(option.slaves, slave)
		option.weight = append(option.weight, weight)
		if weight > 0 {
			option.isWeight = true
		}
	}
}

func XOrmUseRandomPolicy() XOrmGroupOption {
	return func(option *xOrmGroupOption) {
		option.policy = map[int]xorm.GroupPolicy{
			xOrmRandomPolicy: xorm.RandomPolicy(),
		}
	}
}

func XOrmUseLeastConnPolicy() XOrmGroupOption {
	return func(option *xOrmGroupOption) {
		option.policy = map[int]xorm.GroupPolicy{
			xOrmLeastConnPolicy: xorm.LeastConnPolicy(),
		}
	}
}

func XOrmUseRoundRobinPolicy() XOrmGroupOption {
	return func(option *xOrmGroupOption) {
		option.policy = map[int]xorm.GroupPolicy{
			xOrmRoundRobinPolicy: xorm.RoundRobinPolicy(),
		}
	}
}

func initXOrmGroupOptions(options ...XOrmGroupOption) (*xOrmGroupOption, error) {
	defaultPolicy := xorm.RoundRobinPolicy()
	opts := &xOrmGroupOption{realPolicy: defaultPolicy, policy: map[int]xorm.GroupPolicy{
		xOrmRoundRobinPolicy: defaultPolicy,
	}}
	for _, opt := range options {
		opt(opts)
	}
	if opts.name == "" {
		return nil, errors.New("xorm engine group name is empty")
	}
	if opts.master == nil {
		return nil, errors.New("xorm engine master is empty")
	}
	for _, slave := range opts.slaves {
		if slave == nil {
			return nil, errors.New("xorm engine slaves has a empty value")
		}
	}
	// 如果设置权重值,则自动切换GroupPolicy为WeightPolicy
	for k, _ := range opts.policy {
		if opts.isWeight {
			switch k {
			case xOrmRandomPolicy:
				opts.realPolicy = xorm.WeightRandomPolicy(opts.weight)
			case xOrmRoundRobinPolicy, xOrmLeastConnPolicy:
				opts.realPolicy = xorm.WeightRoundRobinPolicy(opts.weight)
			default:
				opts.realPolicy = xorm.WeightRoundRobinPolicy(opts.weight)
			}
		}
	}
	return opts, nil
}

func InitXOrmEngineGroup(options ...XOrmGroupOption) error {
	opts, err := initXOrmGroupOptions(options...)
	if err != nil {
		return err
	}
	if engineGroup, err := xorm.NewEngineGroup(opts.master, opts.slaves, opts.realPolicy); err != nil {
		return err
	} else {
		xOrmGroupRegister(opts.name, engineGroup)
	}
	return nil
}

func xOrmGroupRegister(name string, group *xorm.EngineGroup) {
	xOrmEngineGroup.Store(name, group)
}

func XOrmEngineGroup(group string) *xorm.EngineGroup {
	if engineGroup, ok := xOrmEngineGroup.Load(group); ok && engineGroup != nil {
		return engineGroup.(*xorm.EngineGroup)
	}
	return nil
}

func XOrmEngineMaster(group string) *xorm.Engine {
	if g := XOrmEngineGroup(group); g != nil {
		return g.Master()
	}
	return nil
}

func XOrmEngineSlave(group string) *xorm.Engine {
	if g := XOrmEngineGroup(group); g != nil {
		return g.Slave()
	}
	return nil
}

func XOrmEngineSlaves(group string) []*xorm.Engine {
	if g := XOrmEngineGroup(group); g != nil {
		return g.Slaves()
	}
	return nil
}
