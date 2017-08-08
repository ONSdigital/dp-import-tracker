package main

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/ONSdigital/dp-import-tracker/api"
	"github.com/ONSdigital/dp-import-tracker/schema"
	"github.com/ONSdigital/go-ns/kafka"
	"github.com/ONSdigital/go-ns/log"
	"github.com/ian-kent/gofigure"
)

type config struct {
	NewInstanceTopic          string   `env:"INPUT_FILE_AVAILABLE_TOPIC" flag:"input-file-available-topic" flagDesc:"topic name for import file available events"`
	ObservationsInsertedTopic string   `env:"IMPORT_OBSERVATIONS_INSERTED_TOPIC" flag:"observations-inserted-topic" flagDesc:"topic name for increment of inserted observations"`
	Brokers                   []string `env:"KAFKA_ADDR", flag:"kafka-addr" flagDesc:"topic name for import file available events")`
	ImportAddr                string   `env:"IMPORT_ADDR" flag:"import-addr" flagDesc:"The address of Import API"`
	ImportAuthToken           string   `env:"IMPORT_AUTH_TOKEN" flag:"import-auth-token" flagDesc:"Authentication token for access to import API"`
	DbSource                  string   `env:"DB_ACCESS", flag:"db" flagDesc:"Database access URL/config")`
}

type inputFileAvailable struct {
	FileURL    string `avro:"file_url"`
	InstanceId string `avro:"instance_id"`
}

type InsertedObservationsEvent struct {
	InstanceID                   string `json:"instance_id" avro:"instance_id"`
	NumberOfObservationsInserted int32  `json:"number_of_inserted_observations" avro:"observations_inserted"`
}

type trackedInstance struct {
	totalObservations         int64
	observationsInsertedCount int64
	jobID                     string
}

type trackedInstanceList map[string]trackedInstance

var checkForCompleteInstancesTick = time.Millisecond * 5600

// updateInstanceFromImportAPI updates a specific import instance with the current counts of expected/complete observations
func (trackedInstances trackedInstanceList) updateInstanceFromImportAPI(client *http.Client, importAPIURL string, instanceID string) error {
	instanceFromAPI, err := api.GetInstance(client, importAPIURL, instanceID)
	if err != nil {
		return err
	}

	tempCopy := trackedInstances[instanceID]

	if obsCount := instanceFromAPI.NumberOfObservations; obsCount > 0 && obsCount != trackedInstances[instanceID].totalObservations {
		tempCopy.totalObservations = obsCount
	}
	if observationsInsertedCount := instanceFromAPI.TotalInsertedObservations; observationsInsertedCount > 0 && observationsInsertedCount != trackedInstances[instanceID].observationsInsertedCount {
		tempCopy.observationsInsertedCount = observationsInsertedCount
	}
	if importID := instanceFromAPI.Job.ID; importID != trackedInstances[instanceID].jobID {
		tempCopy.jobID = importID
	}
	trackedInstances[instanceID] = tempCopy
	return nil
}

// getInstanceListFromImportAPI gets a list of current import Instances, to seed our in-memory list
func (trackedInstances trackedInstanceList) getInstanceListFromImportAPI(client *http.Client, importAPIURL string) error {
	instancesFromAPI, err := api.GetInstances(client, importAPIURL, url.Values{"instance_states": []string{"created"}})
	if err != nil {
		return err
	}
	log.Debug("instances", log.Data{"api": instancesFromAPI})
	for _, instance := range instancesFromAPI {
		instanceID := instance.InstanceID
		trackedInstances[instanceID] = trackedInstance{
			totalObservations:         instance.NumberOfObservations,
			observationsInsertedCount: instance.TotalInsertedObservations,
			jobID: instance.Job.ID,
		}
	}
	return nil
}

// CheckImportJobCompletionState checks all instances for given import job - if all completed, mark import as completed
func CheckImportJobCompletionState(client *http.Client, importAPIURL string, jobID, completedInstanceID string) error {
	importJobFromAPI, err := api.GetImportJob(client, importAPIURL, jobID)
	if err != nil {
		return err
	}

	targetState := "completed"
	log.Debug("checking", log.Data{"API insts": importJobFromAPI.Instances})
	for _, instance := range importJobFromAPI.Instances {
		if instance.InstanceID == completedInstanceID {
			continue
		}
		// XXX TODO code below largely untested, and possibly subject to race conditions
		instanceFromAPI, err := api.GetInstance(client, importAPIURL, instance.InstanceID)
		if err != nil {
			return err
		}
		if instanceFromAPI.State != "completed" && instanceFromAPI.State != "error" {
			return nil
		}
		if instanceFromAPI.State == "error" {
			targetState = instanceFromAPI.State
		}
	}
	// assert: all instances for jobID are marked "completed"/"error", so update import as same
	if err := api.UpdateImportJobState(client, importAPIURL, jobID, targetState); err != nil {
		log.ErrorC("Failed to set import job state=completed", err, log.Data{"jobID": jobID, "last completed instanceID": completedInstanceID})
	}
	return nil
}

// updateInstanceWithObservationsInserted updates a specific import instance with the counts of inserted observations
func updateInstanceWithObservationsInserted(client *http.Client, importAPIURL string, instanceID string, observationsInserted int32) error {
	if err := api.UpdateInstanceWithNewInserts(client, importAPIURL, instanceID, observationsInserted); err != nil {
		return err
	}
	return nil
}

// manageActiveInstanceEvents handles all updates to trackedInstances in one thread (this is only called once, in its own thread)
func manageActiveInstanceEvents(createInstanceChan chan string, updateInstanceWithObservationsInsertedChan chan InsertedObservationsEvent, importAPIURL string, client *http.Client) {
	trackedInstances := make(trackedInstanceList)
	if err := trackedInstances.getInstanceListFromImportAPI(client, importAPIURL); err != nil {
		logFatal("Could not obtain initial instance list", err, nil)
	}

	checkForCompletedInstancesChan := make(chan bool)
	go func() {
		for _ = range time.Tick(checkForCompleteInstancesTick) {
			checkForCompletedInstancesChan <- true
		}
	}()

	for {
		select {
		case newInstanceMsg := <-createInstanceChan:
			if _, ok := trackedInstances[newInstanceMsg]; ok {
				log.Error(errors.New("import instance exists"), log.Data{"instanceID": newInstanceMsg})
			} else {
				log.Debug("new import instance", log.Data{"instanceID": newInstanceMsg})
				trackedInstances[newInstanceMsg] = trackedInstance{
					totalObservations:         -1,
					observationsInsertedCount: 0,
				}
			}
		case updateObservationsInserted := <-updateInstanceWithObservationsInsertedChan:
			instanceID := updateObservationsInserted.InstanceID
			if _, ok := trackedInstances[instanceID]; !ok {
				log.Info("Warning: import instance not in tracked list for update", log.Data{"update": updateObservationsInserted})
			}
			log.Debug("updating import instance", log.Data{"update": updateObservationsInserted})
			if err := updateInstanceWithObservationsInserted(client, importAPIURL, instanceID, updateObservationsInserted.NumberOfObservationsInserted); err != nil {
				log.ErrorC("failed to add inserts to instance", err, log.Data{"update": updateObservationsInserted})
			}
		case <-checkForCompletedInstancesChan:
			log.Debug("check import Instances", log.Data{"q": trackedInstances})
			for instanceID, _ := range trackedInstances {
				if err := trackedInstances.updateInstanceFromImportAPI(client, importAPIURL, instanceID); err != nil {
					log.ErrorC("could not update instance", err, log.Data{"instanceID": instanceID})
				} else if trackedInstances[instanceID].totalObservations > 0 && trackedInstances[instanceID].observationsInsertedCount >= trackedInstances[instanceID].totalObservations {
					log.Debug("import instance complete", log.Data{"instanceID": instanceID, "observations": trackedInstances[instanceID].observationsInsertedCount})
					if err := api.UpdateInstanceState(client, importAPIURL, instanceID, "completed"); err != nil {
						log.ErrorC("Failed to set import instance state=completed", err, log.Data{"instanceID": instanceID, "observations": trackedInstances[instanceID].observationsInsertedCount})
					}
					CheckImportJobCompletionState(client, importAPIURL, trackedInstances[instanceID].jobID, instanceID)
					delete(trackedInstances, instanceID)
				}
			}
		}
	}
}

func logFatal(context string, err error, data log.Data) {
	log.ErrorC(context, err, data)
	panic(err)
}

func main() {
	log.Namespace = "dp-import-tracker"

	cfg := config{
		NewInstanceTopic:          "input-file-available",
		ObservationsInsertedTopic: "import-observations-inserted",
		Brokers:                   []string{"localhost:9092"},
		ImportAddr:                "http://localhost:21800",
		DbSource:                  "user=dp dbname=ImportJobs sslmode=disable",
	}
	if err := gofigure.Gofigure(&cfg); err != nil {
		logFatal("gofigure failed", err, nil)
	}
	api.AuthToken = cfg.ImportAuthToken

	log.Info("Starting", log.Data{
		"new-import-topic":        cfg.NewInstanceTopic,
		"completed-inserts-topic": cfg.ObservationsInsertedTopic,
		"import-api":              cfg.ImportAddr,
	})
	newInstanceEventConsumer, err := kafka.NewConsumerGroup(cfg.Brokers, cfg.NewInstanceTopic, log.Namespace, kafka.OffsetNewest)
	if err != nil {
		logFatal("Could not obtain consumer", err, nil)
	}
	observationsInsertedEventConsumer, err := kafka.NewConsumerGroup(cfg.Brokers, cfg.ObservationsInsertedTopic, log.Namespace, kafka.OffsetNewest)
	if err != nil {
		logFatal("Could not obtain consumer", err, log.Data{"topic": cfg.ObservationsInsertedTopic})
	}

	client := &http.Client{}
	updateInstanceWithObservationsInsertedChan := make(chan InsertedObservationsEvent)
	createInstanceChan := make(chan string)
	go manageActiveInstanceEvents(createInstanceChan, updateInstanceWithObservationsInsertedChan, cfg.ImportAddr, client)

	// loop over consumers/producer(error) messages
	for {
		select {
		case newInstanceMessage := <-newInstanceEventConsumer.Incoming():
			var newInstanceEvent inputFileAvailable
			fmt.Printf("msg: %v (%s)", newInstanceMessage.GetData(), newInstanceMessage.GetData())
			if err := schema.InputFileAvailableSchema.Unmarshal(newInstanceMessage.GetData(), &newInstanceEvent); err != nil {
				log.ErrorC("TODO handle unmarshal error", err, log.Data{"topic": cfg.NewInstanceTopic})
			} else {
				createInstanceChan <- newInstanceEvent.InstanceId
			}
			newInstanceMessage.Commit()
		case newImportConsumerErrorMessage := <-newInstanceEventConsumer.Errors():
			log.Error(errors.New("Aborting after consumer error"), log.Data{"error": newImportConsumerErrorMessage, "topic": cfg.NewInstanceTopic})
			break
		case insertedMessage := <-observationsInsertedEventConsumer.Incoming():
			var insertedUpdate InsertedObservationsEvent
			if err := schema.ObservationsInsertedEvent.Unmarshal(insertedMessage.GetData(), &insertedUpdate); err != nil {
				log.ErrorC("unmarshal error", err, log.Data{"topic": cfg.ObservationsInsertedTopic, "msg": insertedMessage})
			} else {
				updateInstanceWithObservationsInsertedChan <- insertedUpdate
			}
			insertedMessage.Commit()
		case insertedObservationsErrorMessage := <-observationsInsertedEventConsumer.Errors():
			log.Error(errors.New("Aborting after consumer error"), log.Data{"error": insertedObservationsErrorMessage, "topic": cfg.ObservationsInsertedTopic})
			break
		}
	}

	// assert: only get here when we have an error, which has been logged
	observationsInsertedEventConsumer.Closer() <- true
	newInstanceEventConsumer.Closer() <- true
	panic("Aborting after error")
}
