package main

import (
	"flag"
	"github.com/ONSdigital/dp-import/events"
	"github.com/ONSdigital/go-ns/kafka"
	"time"
)

var instanceID = flag.String("instance", "5156253b-e21e-4a73-a783-fb53fabc1211", "")
var observationsInserted = flag.Int("observations-inserted", 1, "")

var topic = flag.String("topic", "import-observations-inserted", "")
var kafkaHost = flag.String("kafka", "localhost:9092", "")

func main() {

	flag.Parse()

	var brokers []string
	brokers = append(brokers, *kafkaHost)

	producer, _ := kafka.NewProducer(brokers, *topic, int(2000000))

	event := events.ObservationsInserted{
		InstanceID:           *instanceID,
		ObservationsInserted: int32(*observationsInserted),
	}

	bytes, error := events.ObservationsInsertedSchema.Marshal(event)
	if error != nil {
		panic(error)
	}
	producer.Output() <- bytes

	time.Sleep(time.Duration(time.Second))

	producer.Close(nil)
}
