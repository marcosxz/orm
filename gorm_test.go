package orm

import (
	"github.com/jinzhu/gorm"
	"os"
	"testing"
	"time"
)

type gOrmTestBean struct {
	gorm.Model
	A string  `gorm:"a"`
	B float64 `gorm:"b"`
	C bool    `gorm:"c"`
	V int     `gorm:"version"`
}

func TestInitGOrmDB(t *testing.T) {
	if err := InitGOrmDB(
		GOrmName("default"),
		GOrmDriver("mysql"),
		GOrmDataSource("root:root@tcp(127.0.0.1:3306)/test?charset=utf8"),
		GOrmMaxIdleConn(10),
		GOrmMaxOpenConn(10),
		GOrmLogger(os.Stderr),
		GOrmConnMaxLifetime(time.Second*60),
		GOrmShowSQL(true),
		GOrmAutoMigrate(&gOrmTestBean{}),
		GOrmOpenRedisCache([]string{"127.0.0.1:6379"}, "", 5, time.Second*60),
	); err != nil {
		t.Error(err)
		t.FailNow()
	}
}

func TestGOrmDB(t *testing.T) {
	TestInitGOrmDB(t)
	db := GOrmDB("default")
	defer db.Close()
	db = db.Create(&gOrmTestBean{
		A: "a",
		B: 1.1,
		C: true,
		V: 1,
	})
	if db.Error != nil {
		t.Error(db.Error)
		t.FailNow()
	}

	var bean gOrmTestBean
	db = db.Where("a = ?", "a").First(&bean)
	if db.Error != nil {
		t.Error(db.Error)
		t.FailNow()
	}
	t.Log("gorm get bean1:", bean)

	var bean2 gOrmTestBean
	db2 := GOrmDB("default")
	defer db2.Close()
	db2 = db2.Where("a = ?", "a").First(&bean2)
	if db2.Error != nil {
		t.Error(db2.Error)
		t.FailNow()
	}
	t.Log("gorm get bean2:", bean2)
}
