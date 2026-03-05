package services

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// EventPublisher публикует события об изменении датчиков в RabbitMQ.
// Если RabbitMQ недоступен — просто логируем ошибку, не ломаем основную логику.
type EventPublisher struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	queue   string
}

type SensorEvent struct {
	SensorID  int       `json:"sensor_id"`
	Value     float64   `json:"value"`
	Status    string    `json:"status"`
	Timestamp time.Time `json:"timestamp"`
}

func NewEventPublisher() *EventPublisher {
	url := os.Getenv("RABBITMQ_URL")
	if url == "" {
		url = "amqp://guest:guest@rabbitmq:5672/"
	}

	p := &EventPublisher{queue: "sensor.events"}

	conn, err := amqp.Dial(url)
	if err != nil {
		log.Printf("RabbitMQ not available, events will not be published: %v", err)
		return p
	}

	ch, err := conn.Channel()
	if err != nil {
		log.Printf("Failed to open RabbitMQ channel: %v", err)
		conn.Close()
		return p
	}

	_, err = ch.QueueDeclare(p.queue, true, false, false, false, nil)
	if err != nil {
		log.Printf("Failed to declare queue: %v", err)
		ch.Close()
		conn.Close()
		return p
	}

	p.conn = conn
	p.channel = ch
	log.Println("Connected to RabbitMQ successfully")
	return p
}

func (p *EventPublisher) Publish(sensorID int, value float64, status string) {
	if p.channel == nil {
		return
	}

	event := SensorEvent{
		SensorID:  sensorID,
		Value:     value,
		Status:    status,
		Timestamp: time.Now(),
	}

	body, err := json.Marshal(event)
	if err != nil {
		log.Printf("Failed to marshal event: %v", err)
		return
	}

	err = p.channel.Publish("", p.queue, false, false, amqp.Publishing{
		ContentType: "application/json",
		Body:        body,
	})
	if err != nil {
		log.Printf("Failed to publish event: %v", err)
		return
	}

	log.Printf("Published event for sensor %d: value=%.2f status=%s", sensorID, value, status)
}

func (p *EventPublisher) Close() {
	if p.channel != nil {
		p.channel.Close()
	}
	if p.conn != nil {
		p.conn.Close()
	}
	fmt.Println("EventPublisher closed")
}
