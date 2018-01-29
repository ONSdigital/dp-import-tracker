package main

import (
	"github.com/ONSdigital/dp-import/events"
	"github.com/ONSdigital/go-ns/kafka"
	"time"
	"flag"
)

var instanceID = flag.String("instance", "ac280d98-7211-4b04-9497-40f199396cc3", "")
var jobID = flag.String("job", "cd8759b7-a2f7-49f1-9b56-245d848d50fc", "")
var url = flag.String("url", "https://s3-eu-west-1.amazonaws.com/dp-frontend-florence-file-uploads/159-coicopcomb-inc-geo_cutcsv", "")

var topic = flag.String("topic", "input-file-available", "")
var kafkaHost = flag.String("kafka", "localhost:9092", "")

func main() {

	flag.Parse()

	var brokers []string
	brokers = append(brokers, *kafkaHost)

	producer, _ := kafka.NewProducer(brokers, *topic, int(2000000))

	fileEvent := events.InputFileAvailable{
		InstanceID: *instanceID,
		JobID:      *jobID,
		URL:        *url,
	}

	bytes, error := events.InputFileAvailableSchema.Marshal(fileEvent)
	if error != nil {
		panic(error)
	}
	producer.Output() <- bytes

	time.Sleep(time.Duration(time.Second))

	producer.Close(nil)
}
