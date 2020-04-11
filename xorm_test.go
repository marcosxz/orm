package orm

import (
	"fmt"
	"os"
	"testing"
	"time"
)

type String string
type Slice []string

type xOrmTestBean struct {
	ID int64   `xorm:"id pk autoincr"`
	A  String  `xorm:"a"`
	B  float64 `xorm:"b"`
	C  bool    `xorm:"c"`
	D  Slice   `xorm:"d"`
	V  int     `xorm:"version"`
}

func TestInitXOrmEngine(t *testing.T) {
	if err := InitXOrmEngine(
		XOrmEngineName("default"),
		XOrmDriver("mysql"),
		XOrmDataSource("root:root@tcp(127.0.0.1:3306)/test?charset=utf8"),
		XOrmMaxIdleConn(10),
		XOrmMaxOpenConn(10),
		XOrmLogger(os.Stderr),
		XOrmConnMaxLifetime(time.Second*60),
		XOrmMapper(XOrmSnakeMapper),
		XOrmShowSQL(true),
		XOrmDefaultCache(NewXOrmRedisCache([]string{"127.0.0.1:6379"}, "", 10, time.Second*60)),
		XOrmSync(&xOrmTestBean{}),
	); err != nil {
		t.Error(err)
		t.FailNow()
	}

	t.Log("InitXOrmEngine Successful")
}

func TestXOrmEngine(t *testing.T) {
	TestInitXOrmEngine(t)
	if engine := XOrmEngine("default"); engine == nil {
		t.Error("engine is empty")
		t.FailNow()
	} else {
		t.Logf("XOrmEngine Successful: %v \n", engine)
	}
}

func TestXOrmSession(t *testing.T) {
	TestInitXOrmEngine(t)
	if session := XOrmSession("default"); session == nil {
		t.Error("session is empty")
		t.FailNow()
	} else {
		t.Logf("XOrmSession Successful: %v \n", session)
		defer session.Close()

		// 查询
		var res1 xOrmTestBean
		if _, err := session.Where("id = ?", 1).Get(&res1); err != nil {
			t.Error(err)
			t.FailNow()
		}
		fmt.Println("------------------------", res1)

		// 更新
		if _, err := session.Update(&xOrmTestBean{
			ID: res1.ID,
			A:  res1.A,
			B:  res1.B,
			C:  res1.C,
			D:  res1.D,
			V:  res1.V,
		}); err != nil {
			t.Error(err)
			t.FailNow()
		}

		//  再查询
		var res2 []xOrmTestBean
		if _, err := session.Where("id = ?", 1).FindAndCount(&res2); err != nil {
			t.Error(err)
			t.FailNow()
		}
		fmt.Println("------------------------", res2)

		//  再查询
		var res3 []xOrmTestBean
		if _, err := session.Where("id = ?", 1).FindAndCount(&res3); err != nil {
			t.Error(err)
			t.FailNow()
		}
		fmt.Println("------------------------", res3)
	}
}

func TestInitXOrmEngineGroup(t *testing.T) {
	// master
	if err := InitXOrmEngine(
		XOrmEngineName("master"),
		XOrmDriver("mysql"),
		XOrmDataSource("root:root@tcp(127.0.0.1:3306)/test?charset=utf8"),
		XOrmMaxIdleConn(10),
		XOrmMaxOpenConn(10),
		XOrmConnMaxLifetime(time.Second*60),
		XOrmMapper(XOrmSnakeMapper),
		XOrmSync(&xOrmTestBean{}),
	); err != nil {
		t.Error(err)
		t.FailNow()
	} else {
		t.Log("InitXOrmEngine Master Successful")
	}

	// slave1
	if err := InitXOrmEngine(
		XOrmEngineName("slave1"),
		XOrmDriver("mysql"),
		XOrmDataSource("root:root@tcp(127.0.0.1:3307)/test?charset=utf8"),
		XOrmMaxIdleConn(10),
		XOrmMaxOpenConn(10),
		XOrmConnMaxLifetime(time.Second*60),
		XOrmMapper(XOrmSnakeMapper),
		XOrmSync(&xOrmTestBean{}),
	); err != nil {
		t.Error(err)
		t.FailNow()
	} else {
		t.Log("InitXOrmEngine Slave1 Successful")
	}

	// slave2
	if err := InitXOrmEngine(
		XOrmEngineName("slave2"),
		XOrmDriver("mysql"),
		XOrmDataSource("root:root@tcp(127.0.0.1:3308)/test?charset=utf8"),
		XOrmMaxIdleConn(10),
		XOrmMaxOpenConn(10),
		XOrmConnMaxLifetime(time.Second*60),
		XOrmMapper(XOrmSnakeMapper),
		XOrmSync(&xOrmTestBean{}),
	); err != nil {
		t.Error(err)
		t.FailNow()
	} else {
		t.Log("InitXOrmEngine Slave2 Successful")
	}

	if err := InitXOrmEngineGroup(
		XOrmGroupName("default"),
		XOrmMaster(XOrmEngine("master")),
		XOrmSlave(XOrmEngine("slave1"), 1),
		XOrmSlave(XOrmEngine("slave2"), 2),
		XOrmUseRoundRobinPolicy(),
	); err != nil {
		t.Error(err)
		t.FailNow()
	} else {
		t.Log("InitXOrmEngineGroup Successful")
	}
}

func TestXOrmEngineGroup(t *testing.T) {
	TestInitXOrmEngineGroup(t)
	if group := XOrmEngineGroup("default"); group == nil {
		t.Error("group is empty")
		t.FailNow()
	} else {
		t.Logf("XOrmEngineGroup Successful: %v \n", group)
	}
}

func TestXOrmEngineMaster(t *testing.T) {
	TestInitXOrmEngineGroup(t)
	if master := XOrmEngineMaster("default"); master == nil {
		t.Error("XOrmEngineMaster is empty")
		t.FailNow()
	} else {
		t.Logf("XOrmEngineMaster Successful: %v \n", master)
	}
}

func TestXOrmEngineSlave(t *testing.T) {
	TestInitXOrmEngineGroup(t)
	if slave := XOrmEngineSlave("default"); slave == nil {
		t.Error("XOrmEngineSlave is empty")
		t.FailNow()
	} else {
		t.Logf("XOrmEngineSlave Successful: %v \n", slave)
	}
}

func TestXOrmEngineSlaves(t *testing.T) {
	TestInitXOrmEngineGroup(t)
	if slaves := XOrmEngineSlaves("default"); slaves == nil {
		t.Error("XOrmEngineSlaves is empty")
		t.FailNow()
	} else {
		t.Logf("XOrmEngineSlaves Successful: %v \n", slaves)
	}
}
