// Author: Fatma Reyyan SARIKAYA

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/joho/godotenv"
	"github.com/segmentio/kafka-go"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"log"
	"os"
	"time"
)

func main() {
	conn, _ := kafka.DialLeader(context.Background(), "tcp", "localhost:9092", "hb-project", 0)
	conn.SetReadDeadline(time.Now().Add(time.Second * 3))

	batch := conn.ReadBatch(1e3, 1e9)
	bytesValue := make([]byte, 1e3)

	for {
		_, err := batch.Read(bytesValue)
		if err != nil {
			break
		}
	}
	bytesValue = bytes.Trim(bytesValue, "\x00")
	addMetricToDB(string(bytesValue))
}

func addMetricToDB(jsonMetrics string) {
	// The purpose of this function is to insert metric json to Mongodb

	var jsonMap interface{}
	if err := json.Unmarshal([]byte(jsonMetrics), &jsonMap); err != nil {
		log.Fatal(err)
	}

	// load .env file from given path
	err := godotenv.Load(".env")

	if err != nil {
		log.Fatalf("Error loading .env file")
	}

	mongoUri := os.Getenv("MONGODB_URI")
	client, err := mongo.NewClient(options.Client().ApplyURI(mongoUri))
	if err != nil {
		log.Fatal(err)
	}
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	err = client.Connect(ctx)
	if err != nil {
		log.Fatal(err)
	}

	defer client.Disconnect(ctx)
	if err = client.Ping(ctx, readpref.Primary()); err != nil {
		log.Fatalf("ping failed: %v", err)
	}

	quickstartDatabase := client.Database("hb_project")
	podcastsCollection := quickstartDatabase.Collection("metrics")
	podcastResult, err := podcastsCollection.InsertOne(ctx, jsonMap)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(podcastResult.InsertedID)
}
