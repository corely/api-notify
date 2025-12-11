package database

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"

	_ "github.com/go-sql-driver/mysql" // 导入MySQL驱动
)

// DB 数据库连接全局变量
var DB *sql.DB

// Message 定义消息表结构
type Message struct {
	RequestID string              `json:"request_id"`
	PartnerID string              `json:"partner_id"`
	Endpoints []Endpoint          `json:"endpoints"`
	Headers   map[string][]string `json:"headers"`
	Body      string              `json:"body"`
	Status    string              `json:"status"`
}

// Endpoint 表示一个对外合作方的连接参数
type Endpoint struct {
	IP   string `json:"ip"`
	Port int    `json:"port"`
}

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

// StoreMessage 存储消息到数据库
func StoreMessage(msg Message) error {
	// 将headers转换为JSON字符串存储
	headersJSON, err := json.Marshal(msg.Headers)
	if err != nil {
		return fmt.Errorf("failed to marshal headers: %v", err)
	}

	// 将endpoints转换为JSON字符串存储
	endpointsJSON, err := json.Marshal(msg.Endpoints)
	if err != nil {
		return fmt.Errorf("failed to marshal endpoints: %v", err)
	}

	// 执行插入操作
	_, err = DB.Exec(`
		INSERT INTO message(request_id, partner_id, endpoints, headers, body, status)
		VALUES(?, ?, ?, ?, ?, ?)`,
		msg.RequestID,
		msg.PartnerID,
		string(endpointsJSON),
		string(headersJSON),
		msg.Body,
		msg.Status)

	if err != nil {
		// 检查是否是唯一键冲突错误
		if isDuplicateKeyError(err) {
			return fmt.Errorf("消息已经接受，请不要重复发送")
		}
		return fmt.Errorf("failed to store message: %v", err)
	}

	return nil
}

// isDuplicateKeyError 检查是否是唯一键冲突错误
func isDuplicateKeyError(err error) bool {
	return err.Error() == "Error 1062: Duplicate entry"
}

// QueryMessageByRequestID 根据request_id查询消息是否存在
func QueryMessageByRequestID(requestID string) (bool, error) {
	var count int
	err := DB.QueryRow("SELECT COUNT(*) FROM message WHERE request_id = ?", requestID).Scan(&count)
	if err != nil {
		return false, fmt.Errorf("failed to query message: %v", err)
	}
	return count > 0, nil
}
