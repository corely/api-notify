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

// sendMsgToEndpoint 向指定地址发送消息
func sendMsgToEndpoint(url string, headers map[string][]string, body string, w http.ResponseWriter) {
	client := &http.Client{}
	req, err := http.NewRequest("POST", url, io.NopCloser(bytes.NewReader([]byte(body))))
	if err != nil {
		log.Printf("Failed to create request for %s: %v", url, err)
		return
	}

	// 设置 headers
	for k, v := range headers {
		for _, val := range v {
			req.Header.Add(k, val)
		}
	}

	resp, err := client.Do(req)
	if err != nil {
		log.Printf("Failed to send message to %s: %v", url, err)
		return
	}
	defer resp.Body.Close()

	// 可选：读取响应内容
	_, _ = io.Copy(io.Discard, resp.Body)
}

// main 函数启动 HTTP 服务
func main() {
	// Kafka配置
	kafkaBrokers := []string{"localhost:9092"} // 默认Kafka broker地址

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
