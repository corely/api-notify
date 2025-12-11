package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"api-notify/database"

	"github.com/IBM/sarama"
)

// NotifyRequest 定义了接收的请求结构
type NotifyRequest struct {
	RequestID string              `json:"request_id"`
	PartnerID string              `json:"partner_id"`
	Endpoints []Endpoint          `json:"endpoints"`
	Headers   map[string][]string `json:"headers"`
	Body      string              `json:"body"`
}

// Endpoint 表示一个对外合作方的连接参数
type Endpoint struct {
	IP   string `json:"ip"`
	Port int    `json:"port"`
}

// KafkaProducer Kafka生产者接口
type KafkaProducer interface {
	SendMessage(topic string, key string, value string) error
	Close()
}

// SaramaProducer Sarama实现的Kafka生产者
type SaramaProducer struct {
	producer sarama.AsyncProducer
}

// SendMessage 发送消息到Kafka
func (sp *SaramaProducer) SendMessage(topic string, key string, value string) error {
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.StringEncoder(value),
	}
	sp.producer.Input() <- msg
	return nil
}

// Close 关闭生产者
func (sp *SaramaProducer) Close() {
	sp.producer.AsyncClose()
}

// NewKafkaProducer 创建新的Kafka生产者
func NewKafkaProducer(brokers []string) (KafkaProducer, error) {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	config.Producer.Return.Successes = true

	producer, err := sarama.NewAsyncProducer(brokers, config)
	if err != nil {
		return nil, err
	}

	return &SaramaProducer{producer: producer}, nil
}

// KafkaConsumer Kafka消费者接口
type KafkaConsumer interface {
	Consume() error
	Close() error
}

// SaramaConsumer Sarama实现的Kafka消费者
type SaramaConsumer struct {
	consumer sarama.Consumer
	topic    string
	groupID  string
}

// NewKafkaConsumer 创建新的Kafka消费者
func NewKafkaConsumer(brokers []string, topic string, groupID string) (KafkaConsumer, error) {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	config.Version = sarama.V2_0_0_0

	consumer, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		return nil, err
	}

	return &SaramaConsumer{
		consumer: consumer,
		topic:    topic,
		groupID:  groupID,
	}, nil
}

// Consume 开始消费消息
func (sc *SaramaConsumer) Consume() error {
	partitions, err := sc.consumer.Partitions(sc.topic)
	if err != nil {
		return err
	}

	for _, partition := range partitions {
		pc, err := sc.consumer.ConsumePartition(sc.topic, partition, sarama.OffsetNewest)
		if err != nil {
			return err
		}

		go func(pc sarama.PartitionConsumer) {
			for msg := range pc.Messages() {
				log.Printf("Received message from Kafka: topic=%s, partition=%d, offset=%d, key=%s",
					msg.Topic, msg.Partition, msg.Offset, string(msg.Key))

				// 处理消息
				err := processKafkaMessage(msg)
				if err != nil {
					log.Printf("Failed to process message: %v", err)
				} else {
					log.Printf("Message processed successfully: request_id=%s", string(msg.Key))
				}
			}
		}(pc)
	}

	return nil
}

// Close 关闭消费者
func (sc *SaramaConsumer) Close() error {
	return sc.consumer.Close()
}

// processKafkaMessage 处理Kafka消息
func processKafkaMessage(msg *sarama.ConsumerMessage) error {
	// 解析消息内容为NotifyRequest结构体
	var req NotifyRequest
	err := json.Unmarshal(msg.Value, &req)
	if err != nil {
		return fmt.Errorf("failed to unmarshal message: %v", err)
	}

	// 根据request_id查询数据库
	exists, err := database.QueryMessageByRequestID(req.RequestID)
	if err != nil {
		return fmt.Errorf("failed to query database: %v", err)
	}

	if exists {
		// 如果存在，则直接ack这条消息（在这个实现中，我们使用自动提交，所以不需要显式ack）
		log.Printf("Message already exists in database: request_id=%s", req.RequestID)
		return nil
	}

	// 否则，向合作方发送HTTP POST请求
	for _, ep := range req.Endpoints {
		url := fmt.Sprintf("http://%s:%d/notify", ep.IP, ep.Port)
		err := sendHTTPRequest(url, req.Headers, req.Body)
		if err != nil {
			log.Printf("Failed to send request to %s: %v", url, err)
			// 可以考虑是否继续发送到其他端点
			continue
		} else {
			log.Printf("Successfully sent request to %s", url)
		}
	}

	eps := make([]database.Endpoint, len(req.Endpoints))
	for i, ep := range req.Endpoints {
		eps[i] = database.Endpoint{IP: ep.IP, Port: ep.Port}
	}
	// 将request_id写入数据库
	msgDB := database.Message{
		RequestID: req.RequestID,
		PartnerID: req.PartnerID,
		Endpoints: eps,
		Headers:   req.Headers,
		Body:      req.Body,
		Status:    "processed",
	}

	err = database.StoreMessage(msgDB)
	if err != nil {
		return fmt.Errorf("failed to store message in database: %v", err)
	}

	return nil
}

// sendHTTPRequest 发送HTTP请求到合作方
func sendHTTPRequest(url string, headers map[string][]string, body string) error {
	client := &http.Client{
		Timeout: 10 * time.Second,
	}

	req, err := http.NewRequest("POST", url, bytes.NewBufferString(body))
	if err != nil {
		return err
	}

	// 设置headers
	for k, v := range headers {
		for _, val := range v {
			req.Header.Add(k, val)
		}
	}

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("received non-2xx status code: %d", resp.StatusCode)
	}

	return nil
}

func handler(w http.ResponseWriter, r *http.Request, producer KafkaProducer) {
	// 检查请求方法
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// 读取请求体
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read request body", http.StatusInternalServerError)
		return
	}
	defer r.Body.Close()

	// 解析 JSON
	var req NotifyRequest
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, "Invalid JSON format", http.StatusBadRequest)
		return
	}

	// 日志输出请求信息
	log.Printf("Received notification: request_id=%s, partner_id=%s", req.RequestID, req.PartnerID)

	// 发送消息到Kafka
	err = producer.SendMessage("messages", req.RequestID, req.Body)
	if err != nil {
		http.Error(w, "Failed to send message to Kafka", http.StatusInternalServerError)
		return
	}

	// 返回成功响应
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, `{"status": "success", "request_id": "%s"}`, req.RequestID)
}

// main 函数启动 HTTP 服务
func main() {
	// 数据库配置
	dbDSN := "notify_db_user:notify_db_pass@tcp(127.0.0.1:3306)/notify_db?parseTime=true"

	// 初始化数据库连接
	err := database.InitDB(dbDSN)
	if err != nil {
		log.Fatal("Failed to initialize database:", err)
	}

	// 假定表已经存在
	// 创建message表
	//err = database.CreateMessageTable(database.DB)
	//if err != nil {
	//	log.Fatal("Failed to create message table:", err)
	//}

	// Kafka配置
	kafkaBrokers := []string{"localhost:9092"} // 默认Kafka broker地址
	kafkaTopic := "messages"
	kafkaGroupID := "api-notify-group"

	// 从环境变量获取Kafka配置，如果没有则使用默认值
	if kafkaBrokersEnv := os.Getenv("KAFKA_BROKERS"); kafkaBrokersEnv != "" {
		kafkaBrokers = strings.Split(kafkaBrokersEnv, ",")
	}

	// 创建Kafka生产者
	producer, err := NewKafkaProducer(kafkaBrokers)
	if err != nil {
		log.Fatal("Failed to create Kafka producer:", err)
	}
	defer producer.Close()

	// 创建Kafka消费者
	consumer, err := NewKafkaConsumer(kafkaBrokers, kafkaTopic, kafkaGroupID)
	if err != nil {
		log.Fatal("Failed to create Kafka consumer:", err)
	}
	defer consumer.Close()

	// 启动消费者goroutine
	go func() {
		log.Println("Starting Kafka consumer...")
		if err := consumer.Consume(); err != nil {
			log.Fatal("Kafka consumer error:", err)
		}
	}()

	// 创建HTTP服务器
	server := &http.Server{
		Addr: ":8080",
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			handler(w, r, producer)
		}),
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	// 优雅关闭处理
	done := make(chan bool)
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-quit
		log.Println("Server is shutting down...")

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := server.Shutdown(ctx); err != nil {
			log.Fatal("Server forced to shutdown:", err)
		}

		close(done)
	}()

	log.Println("Starting server on :8080")
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal("Server failed to start:", err)
	}

	<-done
	log.Println("Server stopped")
}
