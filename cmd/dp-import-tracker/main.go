package main

import (
	"context"
	"errors"
	"net/url"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/ONSdigital/dp-import-tracker/api"
	"github.com/ONSdigital/dp-import-tracker/schema"
	"github.com/ONSdigital/go-ns/kafka"
	"github.com/ONSdigital/go-ns/log"
	"github.com/ONSdigital/go-ns/rchttp"
	"github.com/kelseyhightower/envconfig"
)

type config struct {
	NewInstanceTopic          string        `envconfig:"INPUT_FILE_AVAILABLE_TOPIC"`
	ObservationsInsertedTopic string        `envconfig:"IMPORT_OBSERVATIONS_INSERTED_TOPIC"`
	Brokers                   []string      `envconfig:"KAFKA_ADDR"`
	ImportAPIAddr             string        `envconfig:"IMPORT_API_ADDR"`
	ImportAPIAuthToken        string        `envconfig:"IMPORT_API_AUTH_TOKEN"`
	DatasetAPIAddr            string        `envconfig:"DATASET_API_ADDR"`
	DatasetAPIAuthToken       string        `envconfig:"DATASET_API_AUTH_TOKEN"`
	ShutdownTimeout           time.Duration `envconfig:"SHUTDOWN_TIMEOUT"`
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

// updateInstanceFromDatasetAPI updates a specific import instance with the current counts of expected/complete observations
func (trackedInstances trackedInstanceList) updateInstanceFromDatasetAPI(ctx context.Context, datasetAPI *api.DatasetAPI, instanceID string) error {
	instanceFromAPI, err := datasetAPI.GetInstance(ctx, instanceID)
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
func (trackedInstances trackedInstanceList) getInstanceList(ctx context.Context, api *api.DatasetAPI) error {
	instancesFromAPI, err := api.GetInstances(ctx, url.Values{"state": []string{"submitted"}})
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
func CheckImportJobCompletionState(ctx context.Context, importAPI *api.ImportAPI, datasetAPI *api.DatasetAPI, jobID, completedInstanceID string) error {
	importJobFromAPI, err := importAPI.GetImportJob(ctx, jobID)
	if err != nil {
		return err
	}
	// check for 404 not found (empty importJobFromAPI)
	if importJobFromAPI.JobID == "" {
		return errors.New("API did not recognise jobID")
	}

	targetState := "completed"
	log.Debug("checking", log.Data{"API insts": importJobFromAPI.Links.Instances})
	for _, instanceRef := range importJobFromAPI.Links.Instances {
		if instanceRef.ID == completedInstanceID {
			continue
		}
		// XXX TODO code below largely untested, and possibly subject to race conditions
		instanceFromAPI, err := datasetAPI.GetInstance(ctx, instanceRef.ID)
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
	if err := importAPI.UpdateImportJobState(ctx, jobID, targetState); err != nil {
		log.ErrorC("CheckImportJobCompletionState update", err, log.Data{"jobID": jobID, "last completed instanceID": completedInstanceID})
	}
	return nil
}

// manageActiveInstanceEvents handles all updates to trackedInstances in one thread (this is only called once, in its own thread)
func manageActiveInstanceEvents(
	ctx context.Context,
	createInstanceChan chan string,
	updateInstanceWithObservationsInsertedChan chan insertedObservationsEvent,
	datasetAPI *api.DatasetAPI,
	importAPI *api.ImportAPI,
	instanceLoopDoneChan chan bool) {

	trackedInstances := make(trackedInstanceList)
	if err := trackedInstances.getInstanceList(ctx, datasetAPI); err != nil {
		logFatal("could not obtain initial instance list", err, nil)
	}

	checkForCompletedInstancesChan := make(chan bool)
	go func() {
		for {
			select {
			case <-instanceLoopDoneChan:
				return
			case <-time.Tick(checkForCompleteInstancesTick):
				checkForCompletedInstancesChan <- true
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			// inform main() that we're done
			defer close(instanceLoopDoneChan)
			time.Sleep(5 * time.Second)
			log.Info("Instance loop completed", nil)
			return
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
			if err := datasetAPI.UpdateInstanceWithNewInserts(ctx, instanceID, updateObservationsInserted.NumberOfObservationsInserted); err != nil {
				log.ErrorC("failed to add inserts to instance", err, log.Data{"update": updateObservationsInserted})
			}
		case <-checkForCompletedInstancesChan:
			log.Debug("check import Instances", log.Data{"q": trackedInstances})
			for instanceID := range trackedInstances {
				if err := trackedInstances.updateInstanceFromDatasetAPI(ctx, datasetAPI, instanceID); err != nil {
					log.ErrorC("could not update instance", err, log.Data{"instanceID": instanceID})

				} else if trackedInstances[instanceID].totalObservations > 0 && trackedInstances[instanceID].observationsInsertedCount >= trackedInstances[instanceID].totalObservations {
					// the above check on totalObservations ensures that we have updated this instance from the Import API (non-default value)
					// and that inserts have exceeded the expected
					log.Debug("import instance complete", log.Data{"instanceID": instanceID, "observations": trackedInstances[instanceID].observationsInsertedCount})
					log.Error(errors.New("TODO check db for actual count(insertedObservations) now that instance appears to be completed (kafka-double-counting?)"), nil)
					if err := datasetAPI.UpdateInstanceState(ctx, instanceID, "completed"); err != nil {
						log.ErrorC("failed to set import instance state=completed", err, log.Data{"instanceID": instanceID, "observations": trackedInstances[instanceID].observationsInsertedCount})
					} else if err := CheckImportJobCompletionState(ctx, importAPI, datasetAPI, trackedInstances[instanceID].jobID, instanceID); err != nil {
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

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	cfg := config{
		NewInstanceTopic:          "input-file-available",
		ObservationsInsertedTopic: "import-observations-inserted",
		Brokers:                   []string{"localhost:9092"},
		ImportAPIAddr:             "http://localhost:21800",
		DatasetAPIAddr:            "http://localhost:22000",
		ShutdownTimeout:           10 * time.Second,
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

	client := rchttp.DefaultClient
	importAPI := api.NewImportAPI(client, cfg.ImportAPIAddr, cfg.ImportAPIAuthToken)
	datasetAPI := api.NewDatasetAPI(client, cfg.DatasetAPIAddr, cfg.DatasetAPIAuthToken)

	updateInstanceWithObservationsInsertedChan := make(chan insertedObservationsEvent)
	createInstanceChan := make(chan string)
	instanceLoopControlChan := make(chan bool)
	instanceHandlerContext, instanceHandlerCancel := context.WithCancel(context.Background())
	go manageActiveInstanceEvents(instanceHandlerContext, createInstanceChan, updateInstanceWithObservationsInsertedChan, datasetAPI, importAPI, instanceLoopControlChan)

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
		case sig := <-signals:
			log.Error(errors.New("aborting after signal"), log.Data{"signal": sig.String()})
			break SUCCESS
		case <-instanceLoopControlChan:
			log.Error(errors.New("unexpected instance loop exit"), nil)
			break SUCCESS
		}
	}

	// assert: only get here when we have an error, which has been logged

	// gracefully shutdown the application closing any open resources
	log.Error(errors.New("Shutting down with timeout"), log.Data{"timeout": cfg.ShutdownTimeout})
	waitGroup := int32(0)

	// cancel the context for the instance handler goroutine
	instanceHandlerCancel()
	// background a wait for the instance handler to stop
	atomic.AddInt32(&waitGroup, 1)
	go func() { <-instanceLoopControlChan; atomic.AddInt32(&waitGroup, -1) }()

	// initiate closing the consumer context
	observationsInsertedEventConsumerContext, observationsInsertedEventConsumerCancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	// background contextual-timeout Close() of consumer
	atomic.AddInt32(&waitGroup, 1)
	go func() {
		defer observationsInsertedEventConsumerCancel()
		observationsInsertedEventConsumer.Close(observationsInsertedEventConsumerContext)
		atomic.AddInt32(&waitGroup, -1)
	}()

	// initiate closing the consumer context
	newInstanceEventConsumerContext, newInstanceEventConsumerCancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	// background contextual-timeout Close() of consumer
	atomic.AddInt32(&waitGroup, 1)
	go func() {
		defer newInstanceEventConsumerCancel()
		newInstanceEventConsumer.Close(newInstanceEventConsumerContext)
		atomic.AddInt32(&waitGroup, -1)
	}()

	// setup a timer to zero waitGroup after timeout
	go func() {
		<-time.After(cfg.ShutdownTimeout)
		log.Error(errors.New("timeout while shutting down"), nil)
		atomic.AddInt32(&waitGroup, -atomic.LoadInt32(&waitGroup))
	}()

	for atomic.LoadInt32(&waitGroup) > 0 {
	}
	log.Info("Shutdown complete", nil)
	os.Exit(1)
}
