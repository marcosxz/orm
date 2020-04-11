package orm

import (
	"os"
	"testing"
	"time"
)

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
		GOrmAutoMigrate(&xOrmTestBean{}),
	); err != nil {
		t.Error(err)
		t.FailNow()
	}
}

func TestGOrmDB(t *testing.T) {
	TestInitGOrmDB(t)
	db := GOrmDB("default")
	defer db.Close()
	db.Model(&xOrmTestBean{})
}
