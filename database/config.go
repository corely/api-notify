package database

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/go-sql-driver/mysql" // 导入MySQL驱动
)

// DB 数据库连接全局变量
var DB *sql.DB

// InitDB 初始化数据库连接
func InitDB(dataSourceName string) error {
	db, err := sql.Open("mysql", dataSourceName)
	if err != nil {
		return err
	}

	// 测试数据库连接
	if err = db.Ping(); err != nil {
		return err
	}

	DB = db
	log.Println("Database connected successfully")
	return nil
}
