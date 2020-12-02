package main

import (
	"context"
	"flag"
	"os"
	"time"

	"github.com/ONSdigital/dp-import/events"
	kafka "github.com/ONSdigital/dp-kafka/v2"
	"github.com/ONSdigital/log.go/log"
)

var instanceID = flag.String("instance", "ac280d98-7211-4b04-9497-40f199396cc3", "")
var jobID = flag.String("job", "cd8759b7-a2f7-49f1-9b56-245d848d50fc", "")
var url = flag.String("url", "https://s3-eu-west-1.amazonaws.com/dp-frontend-florence-file-uploads/159-coicopcomb-inc-geo_cutcsv", "")

var topic = flag.String("topic", "input-file-available", "")
var kafkaHost = flag.String("kafka", "localhost:9092", "")

func main() {

	flag.Parse()
	ctx := context.Background()

	var pConfig *kafka.ProducerConfig
	var pChannels *kafka.ProducerChannels

	var brokers []string
	brokers = append(brokers, *kafkaHost)

	producer, err := kafka.NewProducer(ctx, brokers, *topic, pChannels, pConfig)
	if err != nil {
		log.Event(ctx, "Error creating Kafka Producer", log.FATAL, log.Error(err))
		os.Exit(1)
	}

	fileEvent := events.InputFileAvailable{
		InstanceID: *instanceID,
		JobID:      *jobID,
		URL:        *url,
	}

	bytes, err := events.InputFileAvailableSchema.Marshal(fileEvent)
	if err != nil {
		log.Event(ctx, "Error marshalling fileEvent", log.FATAL, log.Error(err))
		os.Exit(1)
	}

	producer.Channels().Output <- bytes

	time.Sleep(time.Duration(time.Second))

	producer.Close(ctx)
}
