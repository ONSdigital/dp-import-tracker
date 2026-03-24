package main

import (
	"context"
	"flag"
	"os"
	"time"

	"github.com/ONSdigital/dp-import/events"
	kafka "github.com/ONSdigital/dp-kafka/v5"
	"github.com/ONSdigital/log.go/v2/log"
)

var instanceID = flag.String("instance", "ac280d98-7211-4b04-9497-40f199396cc3", "")
var jobID = flag.String("job", "cd8759b7-a2f7-49f1-9b56-245d848d50fc", "")
var url = flag.String("url", "https://s3-eu-west-1.amazonaws.com/dp-frontend-florence-file-uploads/159-coicopcomb-inc-geo_cutcsv", "")

var topic = flag.String("topic", "input-file-available", "")
var kafkaHost = flag.String("kafka", "localhost:9092", "")

func main() {

	flag.Parse()
	ctx := context.Background()

	pConfig := &kafka.ProducerConfig{
		Topic:       *topic,
		BrokerAddrs: []string{*kafkaHost},
	}

	producer, err := kafka.NewProducer(ctx, pConfig)
	if err != nil {
		log.Fatal(ctx, "Error creating Kafka Producer", err)
		os.Exit(1)
	}

	fileEvent := events.InputFileAvailable{
		InstanceID: *instanceID,
		JobID:      *jobID,
		URL:        *url,
	}

	bytes, err := events.InputFileAvailableSchema.Marshal(fileEvent)
	if err != nil {
		log.Fatal(ctx, "Error marshalling fileEvent", err)
		os.Exit(1)
	}

	producer.Channels().Output <- kafka.BytesMessage{Value: bytes, Context: ctx}

	time.Sleep(time.Duration(time.Second))

	producer.Close(ctx)
}
