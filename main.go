package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
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

// Handler 处理 /notify 接口的 HTTP POST 请求
func handler(w http.ResponseWriter, r *http.Request) {
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
	log.Printf("Received notification: request_id=%s, partner_id=%s, endpoints=%v", req.RequestID, req.PartnerID, req.Endpoints)

	// 发送消息到每个 endpoint
	for _, ep := range req.Endpoints {
		url := fmt.Sprintf("http://%s:%d/notify", ep.IP, ep.Port)
		sendMsgToEndpoint(url, req.Headers, req.Body, w)
	}

	// 返回成功响应
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, `{"status": "success", "request_id": "%s"}`, req.RequestID)
}

// sendMsgToEndpoint 向指定地址发送消息
func sendMsgToEndpoint(url string, headers map[string][]string, body string, w http.ResponseWriter) {
	client := &http.Client{}
	req, err := http.NewRequest("POST", url, io.NopCloser(io.Reader(bytes.NewReader([]byte(body)))))
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
	log.Printf("Sent to %s, status: %d", url, resp.StatusCode)
}

// main 函数启动 HTTP 服务
func main() {
	http.HandleFunc("/notify", handler)
	log.Println("Starting server on :8080")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatal("Server failed to start:", err)
	}
}
