package main

import (
	"database/sql"
	"encoding/json"
	"log"
	"os"
	"time"

	raven "github.com/getsentry/raven-go"
	"github.com/kshvakov/clickhouse"
	"github.com/streadway/amqp"
	common "gitlab.com/maksimsharnin/attn-common"
)

var (
	createQueries = make(map[string]string)
	insertQueries = make(map[string]string)
	execFunctions = make(map[string]func(*sql.Stmt, *common.RabbitMSG) (sql.Result, error))
)

type (
	config struct {
		ClickHouserAddr string `json:"clickHouseAddr"`
		RabbitMQAddr    string `json:"rabbitMQAddr"`
		SentryDSN       string `json:"sentryDSN"`
	}
)

func getConfig() config {
	file, _ := os.Open("config.json")
	decoder := json.NewDecoder(file)
	appConfig := config{}
	err := decoder.Decode(&appConfig)
	if err != nil {
		failOnError(err, "Failed to read config")
	}
	return appConfig
}

func initEvents() {
	initDisplayEvent()
}

func main() {
	appConfig := getConfig()
	raven.SetDSN(appConfig.SentryDSN)

	conn, err := amqp.Dial(appConfig.RabbitMQAddr)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	connect, err := sql.Open("clickhouse", appConfig.ClickHouserAddr)
	if err != nil {
		failOnError(err, "Failed to open ClickHouse Connection")
	}
	if err := connect.Ping(); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			log.Printf("[%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
		} else {
			log.Println(err)
		}
		return
	}

	initEvents()

	eventTypes := [1]string{"display"}
	for _, event := range eventTypes {
		if _, ok := createQueries[event]; !ok {
			log.Fatalln("Create query not initialized for event " + event)
		}
		if _, ok := insertQueries[event]; !ok {
			log.Fatalln("Insert query not initialized for event " + event)
		}
		if _, ok := execFunctions[event]; !ok {
			log.Fatalln("Exec function not initialized for event " + event)
		}
		_, err := connect.Exec(createQueries[event])
		failOnError(err, "Failed to create table for event "+event)

		go func(event string) {
			msgs, err := ch.Consume(
				event, // queue
				"",    // consumer
				false, // auto-ack
				false, // exclusive
				false, // no-local
				false, // no-wait
				nil,   // args
			)
			failOnError(err, "Failed to register a consumer for "+event)
			buff := safeBuffer{}
			go func(b *safeBuffer) {
				for range time.NewTicker(time.Minute).C {
					if b.Len() > 0 {
						tx, _ := connect.Begin()
						stmt, _ := tx.Prepare(insertQueries[event])

						bufferedMsgs := b.Receive()
						for _, d := range bufferedMsgs {
							msg := common.RabbitMSG{}
							json.Unmarshal([]byte(d.Body), &msg)
							if _, err := execFunctions[event](stmt, &msg); err != nil {
								failOnError(err, "Failed to exec query")
							}
							d.Ack(false)
						}
						if err := tx.Commit(); err != nil {
							failOnError(err, "Failed to commit query")
						}
					}
				}
			}(&buff)
			for d := range msgs {
				buff.Append(d)
			}
		}(event)
	}

	forever := make(chan bool)

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}
