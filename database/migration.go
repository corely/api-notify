package database

import (
	"database/sql"
	"log"
)

// CreateMessageTable 创建message表
func CreateMessageTable(db *sql.DB) error {
	query := `
	CREATE TABLE IF NOT EXISTS message (
		id INT AUTO_INCREMENT PRIMARY KEY,
		request_id VARCHAR(255) UNIQUE NOT NULL,
		partner_id VARCHAR(255) NOT NULL,
		endpoints TEXT NOT NULL,
		headers TEXT NOT NULL,
		body TEXT NOT NULL,
		status VARCHAR(50) NOT NULL,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	)`

	_, err := db.Exec(query)
	if err != nil {
		return err
	}

	log.Println("Message table created successfully")
	return nil
}
