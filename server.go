package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"

	"go-n0nz-linebot-101/consumer"
	"go-n0nz-linebot-101/producer"

	"github.com/joho/godotenv"
	"github.com/labstack/echo"
	"github.com/line/line-bot-sdk-go/linebot"
)

type producedMessage struct {
	Id          string `json:"id"`
	Message     string `json:"message"`
	LineUsrName string `json:"line_usr_name"`
	ReplyToken  string `json:"reply_token"`
}

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	// Initialize linebot client
	client := &http.Client{}
	bot, err := linebot.New(os.Getenv("CHANNEL_SECRET"), os.Getenv("CHANNEL_ACCESS_TOKEN"), linebot.WithHTTPClient(client))
	if err != nil {
		log.Fatal("Line bot client ERROR: ", err)
	}

	// Initialize kafka producer
	if err = producer.InitKafka(); err != nil {
		log.Fatal("Kafka producer ERROR: ", err)
	}

	if err := consumer.InitKafka(); err != nil {
		log.Fatal("Kafka consumer ERROR: ", err)
	}

	topics := "user-messages"

	// Initilaze Echo web server
	e := echo.New()

	// just for testing
	e.GET("/", func(c echo.Context) error {
		return c.String(http.StatusOK, "Hello, World!")
	})

	e.POST("/webhook", func(c echo.Context) error {
		fmt.Print("Received Message!")
		events, err := bot.ParseRequest(c.Request())
		if err != nil {
			// Do something when something bad happened.
		}

		for _, event := range events {
			if event.Type == linebot.EventTypeMessage {
				switch message := event.Message.(type) {
				case *linebot.TextMessage:
					messageJson, _ := json.Marshal(&producedMessage{
						Id:          message.ID,
						Message:     message.Text,
						ReplyToken:  event.ReplyToken,
						LineUsrName: event.Source.UserID,
					})
					producerErr := producer.Produce(topics, string(messageJson))
					if producerErr != nil {
						log.Print("Produce error: ", err)
					} else {
						fmt.Printf("Produced [%s] successfully", message.Text)
						//if _, err = bot.ReplyMessage(event.ReplyToken, linebot.NewTextMessage(messageResponse)).Do(); err != nil {
						//	log.Printf("Producer Reply %s error: %s", event.ReplyToken, err)
						//}
					}
				}
			}
		}
		return c.String(http.StatusOK, "OK!")
	})

	msgChan := make(chan string)
	go func() {
		if err := consumer.Consume(topics, &msgChan); err != nil {
			log.Fatal(err)
		}
	}()

	go func() {
		for {
			select {
			case m, ok := <-msgChan:
				if !ok {
					log.Printf("Message from channel not ok: %v", ok)
					continue
				}
				log.Printf("Message from channel: %s", m)

				var msg producedMessage
				if err := json.Unmarshal([]byte(m), &msg); err != nil {
					fmt.Printf("Unable to parse %s, error: %s", m, err)
					continue
				}

				rsp := fmt.Sprintf("ข้อความของคุณ %s ได้รับการประมวลผลเรียบร้อยแล้ว (%s)",
					msg.LineUsrName, msg.Message)

				log.Printf("replying message: %s", m)

				if _, err = bot.ReplyMessage(msg.ReplyToken, linebot.NewTextMessage(rsp)).Do(); err != nil {
					log.Printf("Consumer Reply %s error: %s", msg.ReplyToken, err)
				}
			}
		}
	}()

	e.Logger.Fatal(e.Start(":1323"))
}
