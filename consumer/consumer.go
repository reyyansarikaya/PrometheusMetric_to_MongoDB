// Author: Fatma Reyyan SARIKAYA

package main

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
	"time"
)

func main() {
	conn, _ := kafka.DialLeader(context.Background(), "tcp", "localhost:9092", "test", 0)
	conn.SetReadDeadline(time.Now().Add(time.Second * 3))

	batch := conn.ReadBatch(1e3, 1e9)
	bytes := make([]byte, 1e3)

	for {
		_, err := batch.Read(bytes)
		if err != nil {
			break
		}
		fmt.Println(string(bytes))
	}
}
