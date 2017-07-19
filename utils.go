package main

import (
	"log"
	"sync"

	raven "github.com/getsentry/raven-go"
	"github.com/streadway/amqp"
)

func failOnError(err error, msg string) {
	if err != nil {
		raven.CaptureError(err, nil)
		log.Fatalf("%s: %s", msg, err)
	}
}

// BoolToUInt8 is converter from bool to int8 (ClickHouse feature)
func BoolToUInt8(v bool) int8 {
	if v {
		return 1
	}
	return 0
}

type safeBuffer struct {
	sync.RWMutex
	items []amqp.Delivery
}

func (cs *safeBuffer) Append(item amqp.Delivery) {
	cs.Lock()
	defer cs.Unlock()
	cs.items = append(cs.items, item)
}

func (cs *safeBuffer) Receive() []amqp.Delivery {
	cs.Lock()
	defer cs.Unlock()
	// clear after receive atomic
	defer func() {
		cs.items = cs.items[:0]
	}()
	return cs.items
}

func (cs *safeBuffer) Len() int {
	cs.Lock()
	defer cs.Unlock()
	return len(cs.items)
}
