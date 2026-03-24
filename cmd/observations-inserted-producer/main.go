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

var instanceID = flag.String("instance", "5156253b-e21e-4a73-a783-fb53fabc1211", "")
var observationsInserted = flag.Int("observations-inserted", 1, "")

var topic = flag.String("topic", "import-observations-inserted", "")
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

	event := events.ObservationsInserted{
		InstanceID:           *instanceID,
		ObservationsInserted: int32(*observationsInserted),
	}

	bytes, err := events.ObservationsInsertedSchema.Marshal(event)
	if err != nil {
		log.Fatal(ctx, "Error marshalling event", err)
		os.Exit(1)
	}

	producer.Channels().Output <- kafka.BytesMessage{Value: bytes, Context: ctx}

	time.Sleep(time.Duration(time.Second))

	producer.Close(ctx)
}
