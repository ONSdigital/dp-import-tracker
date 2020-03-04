package main

import (
	"context"
	"flag"
	"time"

	"github.com/ONSdigital/dp-import/events"
	kafka "github.com/ONSdigital/dp-kafka"
)

var instanceID = flag.String("instance", "5156253b-e21e-4a73-a783-fb53fabc1211", "")
var observationsInserted = flag.Int("observations-inserted", 1, "")

var topic = flag.String("topic", "import-observations-inserted", "")
var kafkaHost = flag.String("kafka", "localhost:9092", "")

func main() {

	flag.Parse()
	ctx := context.Background()

	var brokers []string
	brokers = append(brokers, *kafkaHost)

	producer, _ := kafka.NewProducer(ctx, brokers, *topic, int(2000000), kafka.CreateProducerChannels())

	event := events.ObservationsInserted{
		InstanceID:           *instanceID,
		ObservationsInserted: int32(*observationsInserted),
	}

	bytes, error := events.ObservationsInsertedSchema.Marshal(event)
	if error != nil {
		panic(error)
	}
	producer.Channels().Output <- bytes

	time.Sleep(time.Duration(time.Second))

	producer.Close(ctx)
}
