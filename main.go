package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
)

// ==== 配置 ====
const (
	PulsarURL   = "pulsar+ssl://iot-north-mq.heclouds.com:6651"
	IoTAccessID = "23bVZmLSwquFUWoe9572"
	IoTSecretKey = ""
	TopicName   = "23bVZmLSwquFUWoe9572/iot/event"
	SubName     = "23bVZmLSwquFUWoe9572-sub-go"
)

// ==== 生成 OneNET Token ====
// 逻辑: SHA256Hex(AccessID + SHA256Hex(SecretKey))[4:20]
func generateOneNETToken(accessID, secretKey string) string {
	h := sha256.New()
	h.Write([]byte(secretKey))
	innerHash := hex.EncodeToString(h.Sum(nil))

	prehash := accessID + innerHash

	h.Reset()
	h.Write([]byte(prehash))
	doubleHash := hex.EncodeToString(h.Sum(nil))

	if len(doubleHash) < 20 {
		return doubleHash
	}

	return doubleHash[4:20] // 必需的密码片段
}

func main() {
	// -----------------------
	// 1. 计算 Token
	// -----------------------
	token := generateOneNETToken(IoTAccessID, IoTSecretKey)
	fmt.Printf("Access ID: %s\n", IoTAccessID)
	fmt.Printf("Generated Token: %s\n", token)

	// -----------------------
	// 2. 创建 Pulsar 客户端
	// -----------------------
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:                        PulsarURL,
		Authentication:             pulsar.NewAuthenticationToken(token),
		TLSAllowInsecureConnection: true,
		OperationTimeout:           30 * time.Second,
		ConnectionTimeout:          30 * time.Second,
	})
	if err != nil {
		log.Fatalf("Failed to create Pulsar client: %v", err)
	}
	defer client.Close()
	fmt.Println("✓ Pulsar client created successfully!")

	// -----------------------
	// 3. 创建 Producer 并发送消息
	// -----------------------
	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic: TopicName,
	})
	if err != nil {
		log.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	messageContent := fmt.Sprintf("Go client test message (Token Auth). Time: %s", time.Now().Format(time.RFC3339))
	msgID, err := producer.Send(context.Background(), &pulsar.ProducerMessage{
		Payload: []byte(messageContent),
	})
	if err != nil {
		log.Fatalf("Failed to send message: %v", err)
	}
	fmt.Printf("Message sent successfully, ID: %v\n", msgID)

	// -----------------------
	// 4. 创建 Consumer 并接收消息
	// -----------------------
	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:            TopicName,
		SubscriptionName: SubName,
		Type:             pulsar.Shared,
	})
	if err != nil {
		log.Fatalf("Failed to create consumer: %v", err)
	}
	defer consumer.Close()

	fmt.Println("Waiting for message from Pulsar...")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	msg, err := consumer.Receive(ctx)
	if err != nil {
		log.Fatalf("Failed to receive message: %v", err)
	}
	fmt.Printf("Received message: %s\n", string(msg.Payload()))
	consumer.Ack(msg)
	fmt.Println("Message acknowledged successfully!")
}
