package main

import (
	"errors"
	"fmt"
	"net/url"
	"time"

	"github.com/ONSdigital/dp-import-tracker/api"
	"github.com/ONSdigital/dp-import-tracker/schema"
	"github.com/ONSdigital/go-ns/kafka"
	"github.com/ONSdigital/go-ns/log"
	"github.com/ONSdigital/go-ns/rhttp"
	bolt "github.com/johnnadratowski/golang-neo4j-bolt-driver"
	"github.com/kelseyhightower/envconfig"
)

type config struct {
	NewInstanceTopic          string   `envconfig:"INPUT_FILE_AVAILABLE_TOPIC"`
	ObservationsInsertedTopic string   `envconfig:"IMPORT_OBSERVATIONS_INSERTED_TOPIC"`
	Brokers                   []string `envconfig:"KAFKA_ADDR"`
	ImportAPIAddr             string   `envconfig:"IMPORT_API_ADDR"`
	ImportAPIAuthToken        string   `envconfig:"IMPORT_API_AUTH_TOKEN"`
	DatasetAPIAddr            string   `envconfig:"DATASET_API_ADDR"`
	DatasetAPIAuthToken       string   `envconfig:"DATASET_API_AUTH_TOKEN"`
	DatabaseAddress           string   `envconfig:"DATABASE_ADDRESS"`
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

const countObservationsStmt = "MATCH (o:`_%s_observation`) RETURN COUNT(o)"

// updateInstanceFromDatasetAPI updates a specific import instance with the current counts of expected/complete observations
func (trackedInstances trackedInstanceList) updateInstanceFromDatasetAPI(datasetAPI *api.DatasetAPI, instanceID string) error {
	instanceFromAPI, err := datasetAPI.GetInstance(instanceID)
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
	if importID := instanceFromAPI.Links.Job.ID; importID != trackedInstances[instanceID].jobID {
		tempCopy.jobID = importID
	}
	trackedInstances[instanceID] = tempCopy
	return nil
}

// getInstanceList gets a list of current import Instances, to seed our in-memory list
func (trackedInstances trackedInstanceList) getInstanceList(api *api.DatasetAPI) error {
	instancesFromAPI, err := api.GetInstances(url.Values{"state": []string{"submitted"}})
	if err != nil {
		return err
	}
	log.Debug("instances", log.Data{"api": instancesFromAPI})
	for _, instance := range instancesFromAPI {
		instanceID := instance.InstanceID
		trackedInstances[instanceID] = trackedInstance{
			totalObservations:         instance.NumberOfObservations,
			observationsInsertedCount: instance.TotalInsertedObservations,
			jobID: instance.Links.Job.ID,
		}
	}
	return nil
}

// CheckImportJobCompletionState checks all instances for given import job - if all completed, mark import as completed
func CheckImportJobCompletionState(importAPI *api.ImportAPI, datasetAPI *api.DatasetAPI, jobID, completedInstanceID string) error {
	importJobFromAPI, err := importAPI.GetImportJob(jobID)
	if err != nil {
		return err
	}
	// check for 404 not found (empty importJobFromAPI)
	if importJobFromAPI.JobID == "" {
		return errors.New("CheckImportJobCompletionState ImportAPI did not recognise jobID")
	}

	targetState := "completed"
	log.Debug("checking", log.Data{"API insts": importJobFromAPI.Links.Instances})
	for _, instanceRef := range importJobFromAPI.Links.Instances {
		if instanceRef.ID == completedInstanceID {
			continue
		}
		// XXX TODO code below largely untested, and possibly subject to race conditions
		instanceFromAPI, err := datasetAPI.GetInstance(instanceRef.ID)
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
	if err := importAPI.UpdateImportJobState(jobID, targetState); err != nil {
		log.ErrorC("CheckImportJobCompletionState update", err, log.Data{"jobID": jobID, "last completed instanceID": completedInstanceID})
	}
	return nil
}

func CountInsertedObservations(dbConn bolt.Conn, instanceID string) (count int64, err error) {
	var rowCursor bolt.Rows
	if rowCursor, err = dbConn.QueryNeo(fmt.Sprintf(countObservationsStmt, instanceID), nil); err != nil {
		return
	}
	defer rowCursor.Close()
	rows, _, err := rowCursor.All()
	if err != nil {
		return
	}
	var ok bool
	if count, ok = rows[0][0].(int64); !ok {
		return -1, errors.New("Did not get result from DB")
	}
	return
}

// manageActiveInstanceEvents handles all updates to trackedInstances in one thread (this is only called once, in its own thread)
func manageActiveInstanceEvents(
	createInstanceChan chan string,
	updateInstanceWithObservationsInsertedChan chan insertedObservationsEvent,
	datasetAPI *api.DatasetAPI,
	importAPI *api.ImportAPI,
	dbConn bolt.Conn,
) {

	trackedInstances := make(trackedInstanceList)
	if err := trackedInstances.getInstanceList(datasetAPI); err != nil {
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
			if err := datasetAPI.UpdateInstanceWithNewInserts(instanceID, updateObservationsInserted.NumberOfObservationsInserted); err != nil {
				log.ErrorC("failed to add inserts to instance", err, log.Data{"update": updateObservationsInserted})
			}
		case <-checkForCompletedInstancesChan:
			log.Debug("check import Instances", log.Data{"q": trackedInstances})
			for instanceID := range trackedInstances {
				if err := trackedInstances.updateInstanceFromDatasetAPI(datasetAPI, instanceID); err != nil {
					log.ErrorC("could not update instance", err, log.Data{"instanceID": instanceID})

				} else if trackedInstances[instanceID].totalObservations > 0 && trackedInstances[instanceID].observationsInsertedCount >= trackedInstances[instanceID].totalObservations {
					logData := log.Data{
						"instanceID":         instanceID,
						"observations":       trackedInstances[instanceID].observationsInsertedCount,
						"total_observations": trackedInstances[instanceID].totalObservations,
						"job_id":             trackedInstances[instanceID].jobID,
					}
					// the above check on totalObservations ensures that we have updated this instance from the Import API (non-default value)
					// and that inserts have exceeded the expected
					log.Debug("import instance possibly complete - will check db", logData)

					// check db for actual count(insertedObservations) - avoid kafka-double-counting
					countObservations, err := CountInsertedObservations(dbConn, instanceID)
					if err != nil {
						log.ErrorC("Failed to check db for actual count(insertedObservations) now instance appears to be completed", err, logData)
					} else {
						logData["db_count"] = countObservations
						if countObservations != trackedInstances[instanceID].totalObservations {
							log.Trace("db_count of inserted observations != expected total - will continue to monitor", logData)
						} else if err := datasetAPI.UpdateInstanceState(instanceID, "completed"); err != nil {
							log.ErrorC("failed to set import instance state=completed", err, logData)
						} else if err := CheckImportJobCompletionState(importAPI, datasetAPI, trackedInstances[instanceID].jobID, instanceID); err != nil {
							log.ErrorC("failed to check import job when instance completed", err, logData)
						} else {
							// no errors, so stop tracking the completed instance
							delete(trackedInstances, instanceID)
							log.Trace("db_count of inserted observations was expected total - marked completed, ceased monitoring", logData)
						}
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
		ImportAPIAddr:             "http://localhost:21800",
		ImportAPIAuthToken:        "FD0108EA-825D-411C-9B1D-41EF7727F465",
		DatasetAPIAddr:            "http://localhost:22000",
		DatasetAPIAuthToken:       "FD0108EA-825D-411C-9B1D-41EF7727F465",
		DatabaseAddress:           "bolt://localhost:7687",
	}
	if err := envconfig.Process("", &cfg); err != nil {
		logFatal("gofigure failed", err, nil)
	}

	log.Info("starting", log.Data{
		"new-import-topic":        cfg.NewInstanceTopic,
		"completed-inserts-topic": cfg.ObservationsInsertedTopic,
		"import-api":              cfg.ImportAPIAddr,
		"dataset-api":             cfg.DatasetAPIAddr,
	})
	newInstanceEventConsumer, err := kafka.NewConsumerGroup(cfg.Brokers, cfg.NewInstanceTopic, log.Namespace, kafka.OffsetNewest)
	if err != nil {
		logFatal("could not obtain consumer", err, nil)
	}
	observationsInsertedEventConsumer, err := kafka.NewConsumerGroup(cfg.Brokers, cfg.ObservationsInsertedTopic, log.Namespace, kafka.OffsetNewest)
	if err != nil {
		logFatal("could not obtain consumer", err, log.Data{"topic": cfg.ObservationsInsertedTopic})
	}

	dbConnection, err := bolt.NewDriver().OpenNeo(cfg.DatabaseAddress)
	if err != nil {
		logFatal("could not obtain database connection", err, nil)
	}

	client := rhttp.DefaultClient
	importAPI := api.NewImportAPI(client, cfg.ImportAPIAddr, cfg.ImportAPIAuthToken)
	datasetAPI := api.NewDatasetAPI(client, cfg.DatasetAPIAddr, cfg.DatasetAPIAuthToken)

	updateInstanceWithObservationsInsertedChan := make(chan insertedObservationsEvent)
	createInstanceChan := make(chan string)
	go manageActiveInstanceEvents(createInstanceChan, updateInstanceWithObservationsInsertedChan, datasetAPI, importAPI, dbConnection)

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
