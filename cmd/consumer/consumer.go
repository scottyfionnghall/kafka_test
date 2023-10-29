package main

import (
	"context"
	"encoding/json"
	"fmt"
	"kafka-notify/models"
	"log"
	"net/http"

	"github.com/IBM/sarama"
	"github.com/gin-gonic/gin"
)

const (
	ConsumerGroup      = "notifications-group"
	ConsumerTopic      = "notifications"
	ConsumerPort       = ":8081"
	KafkaServerAddress = "localhost:9092"
)

type Consumer struct {
	store *NotificationStore
}

// placeholder methods to satisfy sarama.ConsumerGroupHandler interface
func (*Consumer) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (*Consumer) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (consumer *Consumer) ConsumeClaim(
	sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		userID := string(msg.Key)
		var notification models.Notification
		err := json.Unmarshal(msg.Value, &notification)
		if err != nil {
			log.Printf("failed to unmarshal notification:%v", err)
			continue
		}
		consumer.store.Add(userID, notification)
		sess.MarkMessage(msg, "")
	}
	return nil
}

func initializeConsumerGroup() (sarama.ConsumerGroup, error) {
	config := sarama.NewConfig()
	consumerGroup, err := sarama.NewConsumerGroup(
		[]string{KafkaServerAddress}, ConsumerGroup, config)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize consumer group: %w", err)
	}

	return consumerGroup, nil
}

func setupConsumerGroup(ctx context.Context, store *NotificationStore) {
	consumerGroup, err := initializeConsumerGroup()
	if err != nil {
		log.Printf("intializtion error: %v", err)
	}
	defer consumerGroup.Close()

	consumer := &Consumer{
		store: store,
	}

	for {
		err = consumerGroup.Consume(ctx, []string{ConsumerTopic}, consumer)
		if err != nil {
			log.Printf("error from consumer: %v", err)
		}
		if ctx.Err() != nil {
			return
		}
	}
}

func handleNotifications(ctx *gin.Context, store *NotificationStore) {
	userID, err := getUserIDFromRequest(ctx)
	if err != nil {
		ctx.JSON(http.StatusNotFound, gin.H{"message": err.Error()})
		return
	}

	notes := store.Get(userID)
	if len(notes) == 0 {
		ctx.JSON(
			http.StatusOK,
			gin.H{
				"message":       "No notifications found for user",
				"notifications": []models.Notification{},
			})
		return
	}

	ctx.JSON(http.StatusOK, gin.H{"notifications": notes})
}

func main() {
	store := &NotificationStore{
		data: make(UserNotifications),
	}

	ctx, cancel := context.WithCancel(context.Background())
	go setupConsumerGroup(ctx, store)
	defer cancel()
	gin.SetMode(gin.ReleaseMode)
	r := gin.Default()
	r.GET("/notifications/:userID", func(ctx *gin.Context) {
		handleNotifications(ctx, store)
	})

	fmt.Printf("Kafka CONSUMER (Group: %s) started at localhost%s\n", ConsumerGroup, ConsumerPort)
	if err := r.Run(ConsumerPort); err != nil {
		log.Printf("failed to run the server: %v", err)
	}
}
