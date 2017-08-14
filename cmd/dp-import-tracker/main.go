package main

import (
	"errors"
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
	Brokers                   []string `env:"KAFKA_ADDR" flag:"kafka-addr" flagDesc:"topic name for import file available events"`
	ImportAddr                string   `env:"IMPORT_ADDR" flag:"import-addr" flagDesc:"The address of Import API"`
	ImportAuthToken           string   `env:"IMPORT_AUTH_TOKEN" flag:"import-auth-token" flagDesc:"Authentication token for access to import API"`
}

type inputFileAvailable struct {
	FileURL    string `avro:"file_url"`
	InstanceID string `avro:"instance_id"`
}

type insertedObservationsEvent struct {
	InstanceID                   string `json:"instance_id" avro:"instance_id"`
	NumberOfObservationsInserted int32  `json:"number_of_inserted_observations" avro:"observations_inserted"`
}

type trackedInstance struct {
	totalObservations         int64
	observationsInsertedCount int64
	jobID                     string
}

type trackedInstanceList map[string]trackedInstance

var checkForCompleteInstancesTick = time.Millisecond * 2000

// updateInstanceFromImportAPI updates a specific import instance with the current counts of expected/complete observations
func (trackedInstances trackedInstanceList) updateInstanceFromImportAPI(api *api.ImportAPI, instanceID string) error {
	instanceFromAPI, err := api.GetInstance(instanceID)
	if err != nil {
		return err
	}
	if instanceFromAPI.InstanceID == "" {
		log.Debug("no such instance at API - removing from tracker", log.Data{"InstanceID": instanceID})
		delete(trackedInstances, instanceID)
		return nil
	}

	tempCopy := trackedInstances[instanceID]

	if obsCount := instanceFromAPI.NumberOfObservations; obsCount > 0 && obsCount != trackedInstances[instanceID].totalObservations {
		tempCopy.totalObservations = obsCount
	}
	if observationsInsertedCount := instanceFromAPI.TotalInsertedObservations; observationsInsertedCount > 0 && observationsInsertedCount != trackedInstances[instanceID].observationsInsertedCount {
		tempCopy.observationsInsertedCount = observationsInsertedCount
	}
	// update jobID (should only 'change' once, on first update after initial creation of the instance - which has no jobID)
	if importID := instanceFromAPI.Job.ID; importID != trackedInstances[instanceID].jobID {
		tempCopy.jobID = importID
	}
	trackedInstances[instanceID] = tempCopy
	return nil
}

// getInstanceListFromImportAPI gets a list of current import Instances, to seed our in-memory list
func (trackedInstances trackedInstanceList) getInstanceListFromImportAPI(api *api.ImportAPI) error {
	instancesFromAPI, err := api.GetInstances(url.Values{"instance_states": []string{"created"}})
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
func CheckImportJobCompletionState(api *api.ImportAPI, jobID, completedInstanceID string) error {
	importJobFromAPI, err := api.GetImportJob(jobID)
	if err != nil {
		return err
	}
	// check for 404 not found (empty importJobFromAPI)
	if importJobFromAPI.JobID == "" {
		return errors.New("API did not recognise jobID")
	}

	targetState := "completed"
	log.Debug("checking", log.Data{"API insts": importJobFromAPI.Instances})
	for _, instanceRef := range importJobFromAPI.Instances {
		if instanceRef.ID == completedInstanceID {
			continue
		}
		// XXX TODO code below largely untested, and possibly subject to race conditions
		instanceFromAPI, err := api.GetInstance(instanceRef.ID)
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
	if err := api.UpdateImportJobState(jobID, targetState); err != nil {
		log.ErrorC("CheckImportJobCompletionState update", err, log.Data{"jobID": jobID, "last completed instanceID": completedInstanceID})
	}
	return nil
}

// updateInstanceWithObservationsInserted updates a specific import instance with the counts of inserted observations
func updateInstanceWithObservationsInserted(api *api.ImportAPI, instanceID string, observationsInserted int32) error {
	if err := api.UpdateInstanceWithNewInserts(instanceID, observationsInserted); err != nil {
		return err
	}
	return nil
}

// manageActiveInstanceEvents handles all updates to trackedInstances in one thread (this is only called once, in its own thread)
func manageActiveInstanceEvents(createInstanceChan chan string, updateInstanceWithObservationsInsertedChan chan insertedObservationsEvent, api *api.ImportAPI) {
	trackedInstances := make(trackedInstanceList)
	if err := trackedInstances.getInstanceListFromImportAPI(api); err != nil {
		logFatal("could not obtain initial instance list", err, nil)
	}

	checkForCompletedInstancesChan := make(chan bool)
	go func() {
		for range time.Tick(checkForCompleteInstancesTick) {
			checkForCompletedInstancesChan <- true
		}
	}()

	for {
		select {
		case newInstanceMsg := <-createInstanceChan:
			if _, ok := trackedInstances[newInstanceMsg]; ok {
				log.Error(errors.New("import instance exists"), log.Data{"instanceID": newInstanceMsg})
				continue
			}
			log.Debug("new import instance", log.Data{"instanceID": newInstanceMsg})
			trackedInstances[newInstanceMsg] = trackedInstance{
				totalObservations:         -1,
				observationsInsertedCount: 0,
			}
		case updateObservationsInserted := <-updateInstanceWithObservationsInsertedChan:
			instanceID := updateObservationsInserted.InstanceID
			if _, ok := trackedInstances[instanceID]; !ok {
				log.Info("warning: import instance not in tracked list for update", log.Data{"update": updateObservationsInserted})
			}
			log.Debug("updating import instance", log.Data{"update": updateObservationsInserted})
			if err := updateInstanceWithObservationsInserted(api, instanceID, updateObservationsInserted.NumberOfObservationsInserted); err != nil {
				log.ErrorC("failed to add inserts to instance", err, log.Data{"update": updateObservationsInserted})
			}
		case <-checkForCompletedInstancesChan:
			log.Debug("check import Instances", log.Data{"q": trackedInstances})
			for instanceID := range trackedInstances {
				if err := trackedInstances.updateInstanceFromImportAPI(api, instanceID); err != nil {
					log.ErrorC("could not update instance", err, log.Data{"instanceID": instanceID})

				} else if trackedInstances[instanceID].totalObservations > 0 && trackedInstances[instanceID].observationsInsertedCount >= trackedInstances[instanceID].totalObservations {
					// the above check on totalObservations ensures that we have updated this instance from the Import API (non-default value)
					// and that inserts have exceeded the expected
					log.Debug("import instance complete", log.Data{"instanceID": instanceID, "observations": trackedInstances[instanceID].observationsInsertedCount})
					log.Error(errors.New("TODO check db for actual count(insertedObservations) now that instance appears to be completed (kafka-double-counting?)"), nil)
					if err := api.UpdateInstanceState(instanceID, "completed"); err != nil {
						log.ErrorC("failed to set import instance state=completed", err, log.Data{"instanceID": instanceID, "observations": trackedInstances[instanceID].observationsInsertedCount})
					} else if err := CheckImportJobCompletionState(api, trackedInstances[instanceID].jobID, instanceID); err != nil {
						log.ErrorC("failed to check import job when instance completed", err, log.Data{"instanceID": instanceID, "jobID": trackedInstances[instanceID].jobID})
					} else {
						// no errors, so stop tracking the completed instance
						delete(trackedInstances, instanceID)
					}

				}
			}
		}
	}
}

// logFatal is a utility method for a common failure pattern in main()
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
	}
	if err := gofigure.Gofigure(&cfg); err != nil {
		logFatal("gofigure failed", err, nil)
	}
	api.AuthToken = cfg.ImportAuthToken

	log.Info("starting", log.Data{
		"new-import-topic":        cfg.NewInstanceTopic,
		"completed-inserts-topic": cfg.ObservationsInsertedTopic,
		"import-api":              cfg.ImportAddr,
	})
	newInstanceEventConsumer, err := kafka.NewConsumerGroup(cfg.Brokers, cfg.NewInstanceTopic, log.Namespace, kafka.OffsetNewest)
	if err != nil {
		logFatal("could not obtain consumer", err, nil)
	}
	observationsInsertedEventConsumer, err := kafka.NewConsumerGroup(cfg.Brokers, cfg.ObservationsInsertedTopic, log.Namespace, kafka.OffsetNewest)
	if err != nil {
		logFatal("could not obtain consumer", err, log.Data{"topic": cfg.ObservationsInsertedTopic})
	}

	client := &http.Client{}
	api, err := api.New(client, cfg.ImportAddr)
	if err != nil {
		logFatal("could not obtain API instance", err, nil)
	}

	updateInstanceWithObservationsInsertedChan := make(chan insertedObservationsEvent)
	createInstanceChan := make(chan string)
	go manageActiveInstanceEvents(createInstanceChan, updateInstanceWithObservationsInsertedChan, api)

	// loop over consumers/producer(error) messages
SUCCESS:
	for {
		select {
		case newInstanceMessage := <-newInstanceEventConsumer.Incoming():
			var newInstanceEvent inputFileAvailable
			if err := schema.InputFileAvailableSchema.Unmarshal(newInstanceMessage.GetData(), &newInstanceEvent); err != nil {
				log.ErrorC("TODO handle unmarshal error", err, log.Data{"topic": cfg.NewInstanceTopic})
			} else {
				createInstanceChan <- newInstanceEvent.InstanceID
			}
			newInstanceMessage.Commit()
		case newImportConsumerErrorMessage := <-newInstanceEventConsumer.Errors():
			log.Error(errors.New("aborting after consumer error"), log.Data{"error": newImportConsumerErrorMessage, "topic": cfg.NewInstanceTopic})
			break SUCCESS
		case insertedMessage := <-observationsInsertedEventConsumer.Incoming():
			var insertedUpdate insertedObservationsEvent
			if err := schema.ObservationsInsertedEvent.Unmarshal(insertedMessage.GetData(), &insertedUpdate); err != nil {
				log.ErrorC("unmarshal error", err, log.Data{"topic": cfg.ObservationsInsertedTopic, "msg": insertedMessage})
			} else {
				updateInstanceWithObservationsInsertedChan <- insertedUpdate
			}
			insertedMessage.Commit()
		case insertedObservationsErrorMessage := <-observationsInsertedEventConsumer.Errors():
			log.Error(errors.New("aborting after consumer error"), log.Data{"error": insertedObservationsErrorMessage, "topic": cfg.ObservationsInsertedTopic})
			break SUCCESS
		}
	}

	// assert: only get here when we have an error, which has been logged
	observationsInsertedEventConsumer.Closer() <- true
	newInstanceEventConsumer.Closer() <- true
	logFatal("", errors.New("aborting after error"), nil)
}
