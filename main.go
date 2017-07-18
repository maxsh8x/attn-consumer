package main

import (
	"database/sql"
	"encoding/json"
	"log"
	"os"

	raven "github.com/getsentry/raven-go"
)

type config struct {
	ClickHouserAddr string `json:"clickHouseAddr"`
	RabbitMQAddr    string `json:"rabbitMQAddr"`
	SentryDSN       string `json:"sentryDSN"`
}

func failOnError(err error, msg string) {
	if err != nil {
		raven.CaptureError(err, nil)
		log.Fatalf("%s: %s", msg, err)
	}
}

func getConfig() config {
	file, _ := os.Open("config.json")
	decoder := json.NewDecoder(file)
	appConfig := config{}
	err := decoder.Decode(&appConfig)
	if err != nil {
		raven.CaptureError(err, nil)
		failOnError(err, "Can't read config")
	}
	return appConfig
}

func main() {
	appConfig := getConfig()
	raven.SetDSN(appConfig.SentryDSN)

	connect, err := sql.Open("clickhouse", appConfig.ClickHouserAddr)
	if err != nil {
		log.Fatal(err)
	}
	err = connect.Ping()
	failOnError(err, "ClickHouse doesn't respond")
}
