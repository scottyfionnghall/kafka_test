package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"kafka-notify/models"
	"log"
	"net/http"
	"os"
	"strconv"

	"github.com/IBM/sarama"
	"github.com/gin-gonic/gin"
	"github.com/joho/godotenv"
)

const (
	ProducerPort       = ":8080"
	KafkaServerAddress = "localhost:9092"
	KafkaTopic         = "notifications"
)

func sendKafkaMessage(producer sarama.SyncProducer, ctx *gin.Context, fromID, toID int) error {
	message := ctx.PostForm("message")

	fromUser, err := findUserByID(fromID)
	if err != nil {
		return err
	}

	toUser, err := findUserByID(toID)
	if err != nil {
		return err
	}

	notification := models.Notification{
		From:    fromUser,
		To:      toUser,
		Message: message,
	}

	notificationJSON, err := json.Marshal(notification)
	if err != nil {
		return fmt.Errorf(
			"failed to marshal notification: %w", err)
	}

	msg := &sarama.ProducerMessage{
		Topic: KafkaTopic,
		Key:   sarama.StringEncoder(strconv.Itoa(toUser.ID)),
		Value: sarama.StringEncoder(notificationJSON),
	}

	_, _, err = producer.SendMessage(msg)
	return err

}

func sendMessageHandler(producer sarama.SyncProducer) gin.HandlerFunc {
	return func(ctx *gin.Context) {
		fromID, err := getIDFromReqest("fromID", ctx)
		if err != nil {
			ctx.JSON(http.StatusBadRequest, gin.H{
				"message": err,
			})
			return
		}

		toID, err := getIDFromReqest("toID", ctx)
		if err != nil {
			ctx.JSON(http.StatusBadRequest, gin.H{
				"message": err,
			})
			return
		}

		err = sendKafkaMessage(producer, ctx, fromID, toID)
		if errors.Is(err, ErrUserNotFoundInProducer) {
			ctx.JSON(http.StatusNotFound, gin.H{
				"message": "User not found",
			})
			return
		}
		if err != nil {
			ctx.JSON(http.StatusInternalServerError, gin.H{
				"message": err.Error(),
			})
			return
		}

		ctx.JSON(http.StatusOK, gin.H{
			"message": "Notification sent successfully!",
		})
	}
}

func setupProducer() (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	producer, err := sarama.NewSyncProducer([]string{KafkaServerAddress}, config)
	if err != nil {
		return nil, fmt.Errorf("failed to setup producer: %w", err)
	}
	return producer, nil
}

func main() {

	err := godotenv.Load(".env")
	if err != nil {
		log.Fatal(err)
		return
	}

	dsn := fmt.Sprintf("host=%s user=%s password=%s dbname=%s port=%s sslmode=%s TimeZone=%s", os.Getenv("DB_HOST"), os.Getenv("DB_USER"), os.Getenv("DB_PASSWORD"), os.Getenv("DB_NAME"), os.Getenv("DB_PORT"), os.Getenv("DB_SSLMODE"), os.Getenv("DB_TIMEZONE"))

	err = models.ConnectDB(dsn)
	if err != nil {
		log.Fatal(err)
		return
	}

	producer, err := setupProducer()
	if err != nil {
		log.Fatalf("failed to initialize producer: %v", err)
	}
	defer producer.Close()

	gin.SetMode(gin.ReleaseMode)
	r := gin.Default()
	r.POST("/send", sendMessageHandler(producer))

	fmt.Printf("Kafka PRODUCER started at localhost:%s\n", ProducerPort)

	if err := r.Run(ProducerPort); err != nil {
		log.Printf("failed to run the server: %v", err)
	}
}
