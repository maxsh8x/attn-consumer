package main

import (
	"database/sql"
	"encoding/json"
	"log"
	"os"

	"fmt"

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
	eventTypes := [3]string{"display", "click", "view"}
	for _, event := range eventTypes {
		connect.Exec(fmt.Sprintf(`
			CREATE TABLE %s (
				date DateTime,
				mobile Boolean,
				platform FixedString(32),
				os FixedString(32),
				vrowser FixedString(32),
				version FixedString(32),
				ip FixedString(16),
				offer_id UInt32,
				placement_id UInt32,
				widget_id UInt32,
				time_spent UInt32,
				scrolled Boolean,
				latitude Float32,
				lontitude Float32
			) engine=Memory
		`, event))
	}
}
