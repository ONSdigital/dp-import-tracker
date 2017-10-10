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
	BindAddr                  string        `envconfig:"BIND_ADDR"`
}

type inputFileAvailable struct {
	FileURL    string `avro:"file_url"`
	InstanceID string `avro:"instance_id"`
}

type insertedObservationsEvent struct {
	InstanceID                   string `json:"instance_id" avro:"instance_id"`
	NumberOfObservationsInserted int32  `json:"number_of_inserted_observations" avro:"observations_inserted"`
	DoneChan                     chan bool
}

type trackedInstance struct {
	totalObservations         int64
	observationsInsertedCount int64
	jobID                     string
}

type trackedInstanceList map[string]trackedInstance

var checkForCompleteInstancesTick = time.Millisecond * 2000

// updateInstanceFromDatasetAPI updates a specific import instance with the current counts of expected/complete observations
func (trackedInstances trackedInstanceList) updateInstanceFromDatasetAPI(ctx context.Context, datasetAPI *api.DatasetAPI, instanceID string) (error, bool) {
	instanceFromAPI, err, isFatal := datasetAPI.GetInstance(ctx, instanceID)
	if err != nil {
		return err, isFatal
	}
	if instanceFromAPI.InstanceID == "" {
		log.Debug("no such instance at API - removing from tracker", log.Data{"InstanceID": instanceID})
		delete(trackedInstances, instanceID)
		return nil, false
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
	return nil, false
}

// getInstanceList gets a list of current import Instances, to seed our in-memory list
func (trackedInstances trackedInstanceList) getInstanceList(ctx context.Context, api *api.DatasetAPI) (error, bool) {
	instancesFromAPI, err, isFatal := api.GetInstances(ctx, url.Values{"state": []string{"submitted"}})
	if err != nil {
		return err, isFatal
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
	return nil, false
}

// CheckImportJobCompletionState checks all instances for given import job - if all completed, mark import as completed
func CheckImportJobCompletionState(ctx context.Context, importAPI *api.ImportAPI, datasetAPI *api.DatasetAPI, jobID, completedInstanceID string) (error, bool) {
	importJobFromAPI, err, isFatal := importAPI.GetImportJob(ctx, jobID)
	if err != nil {
		return err, isFatal
	}
	// check for 404 not found (empty importJobFromAPI)
	if importJobFromAPI.JobID == "" {
		return errors.New("API did not recognise jobID"), false
	}

	targetState := "completed"
	log.Debug("checking", log.Data{"API insts": importJobFromAPI.Links.Instances})
	for _, instanceRef := range importJobFromAPI.Links.Instances {
		if instanceRef.ID == completedInstanceID {
			continue
		}
		// XXX TODO code below largely untested, and possibly subject to race conditions
		instanceFromAPI, err, isFatal := datasetAPI.GetInstance(ctx, instanceRef.ID)
		if err != nil {
			return err, isFatal
		}
		if instanceFromAPI.State != "completed" && instanceFromAPI.State != "error" {
			return nil, false
		}
		if instanceFromAPI.State == "error" {
			targetState = instanceFromAPI.State
		}
	}
	// assert: all instances for jobID are marked "completed"/"error", so update import as same
	if err := importAPI.UpdateImportJobState(ctx, jobID, targetState); err != nil {
		log.ErrorC("CheckImportJobCompletionState update", err, log.Data{"jobID": jobID, "last completed instanceID": completedInstanceID})
	}
	return nil, false
}

// manageActiveInstanceEvents handles all updates to trackedInstances in one thread (this is only called once, in its own thread)
func manageActiveInstanceEvents(
	ctx context.Context,
	createInstanceChan chan string,
	updateInstanceWithObservationsInsertedChan chan insertedObservationsEvent,
	datasetAPI *api.DatasetAPI,
	importAPI *api.ImportAPI,
	instanceLoopDoneChan chan bool) {

	// inform main() (and above ticker goroutine) when we stop processing events
	defer close(instanceLoopDoneChan)

	trackedInstances := make(trackedInstanceList)
	if err, _ := trackedInstances.getInstanceList(ctx, datasetAPI); err != nil {
		logFatal("could not obtain initial instance list", err, nil)
	}

	for looping := true; looping; {
		select {
		case <-ctx.Done():
			log.Debug("Instance loop completing", nil)
			looping = false
		case <-time.Tick(checkForCompleteInstancesTick):
			log.Debug("check import Instances", log.Data{"q": trackedInstances})
			for instanceID := range trackedInstances {
				stopTracking := false
				if err, isFatal := trackedInstances.updateInstanceFromDatasetAPI(ctx, datasetAPI, instanceID); err != nil {
					log.ErrorC("could not update instance", err, log.Data{"instanceID": instanceID, "isFatal": isFatal})
					stopTracking = isFatal
				} else if trackedInstances[instanceID].totalObservations > 0 && trackedInstances[instanceID].observationsInsertedCount >= trackedInstances[instanceID].totalObservations {
					// the above check on totalObservations ensures that we have updated this instance from the Import API (non-default value)
					// and that inserts have exceeded the expected
					logData := log.Data{"instanceID": instanceID, "observations": trackedInstances[instanceID].observationsInsertedCount, "jobID": trackedInstances[instanceID].jobID}
					log.Debug("import instance complete", logData)
					log.Error(errors.New("TODO check db for actual count(insertedObservations) now that instance appears to be completed (kafka-double-counting?)"), nil)
					// assume no error, i.e. that updating worked, so we can stopTracking
					stopTracking = true
					if err, isFatal = datasetAPI.UpdateInstanceState(ctx, instanceID, "completed"); err != nil {
						logData["isFatal"] = isFatal
						log.ErrorC("failed to set import instance state=completed", err, logData)
						stopTracking = isFatal
					} else if err, isFatal = CheckImportJobCompletionState(ctx, importAPI, datasetAPI, trackedInstances[instanceID].jobID, instanceID); err != nil {
						logData["isFatal"] = isFatal
						log.ErrorC("failed to check import job when instance completed", err, logData)
						stopTracking = isFatal
					}
				}
				if stopTracking {
					// no (or fatal) errors, so stop tracking the completed (or failed) instance
					delete(trackedInstances, instanceID)
				}
			}
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
			if err, isFatal := datasetAPI.UpdateInstanceWithNewInserts(ctx, instanceID, updateObservationsInserted.NumberOfObservationsInserted); err != nil {
				log.ErrorC("failed to add inserts to instance", err, log.Data{"update": updateObservationsInserted})
				updateObservationsInserted.DoneChan <- isFatal
			} else {
				close(updateObservationsInserted.DoneChan)
			}
		}
	}
	log.Info("Instance loop completed", nil)
}

// logFatal is a utility method for a common failure pattern in main()
func logFatal(contextMessage string, err error, data log.Data) {
	log.ErrorC(contextMessage, err, data)
	panic(err)
}

func main() {
	log.Namespace = "dp-import-tracker"

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	cfg := config{
		BindAddr:                  ":21300",
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

	httpServerDoneChan := make(chan error)
	api.StartHealthCheck(cfg.BindAddr, httpServerDoneChan)

	newInstanceEventConsumer, err := kafka.NewConsumerGroup(cfg.Brokers, cfg.NewInstanceTopic, log.Namespace, kafka.OffsetNewest)
	if err != nil {
		logFatal("could not obtain consumer", err, nil)
	}
	observationsInsertedEventConsumer, err := kafka.NewConsumerGroup(cfg.Brokers, cfg.ObservationsInsertedTopic, log.Namespace, kafka.OffsetNewest)
	if err != nil {
		logFatal("could not obtain consumer", err, log.Data{"topic": cfg.ObservationsInsertedTopic})
	}

	client := rchttp.DefaultClient
	client.MaxRetries = 4

	importAPI := api.NewImportAPI(client, cfg.ImportAPIAddr, cfg.ImportAPIAuthToken)
	datasetAPI := api.NewDatasetAPI(client, cfg.DatasetAPIAddr, cfg.DatasetAPIAuthToken)

	updateInstanceWithObservationsInsertedChan := make(chan insertedObservationsEvent)
	createInstanceChan := make(chan string)
	instanceLoopEndedChan := make(chan bool)
	instanceHandlerContext, instanceHandlerCancel := context.WithCancel(context.Background())
	go manageActiveInstanceEvents(instanceHandlerContext, createInstanceChan, updateInstanceWithObservationsInsertedChan, datasetAPI, importAPI, instanceLoopEndedChan)

	// loop over consumers messages and errors (or signals or other bad things)
	for looping := true; looping; {
		select {
		case sig := <-signals:
			log.Error(errors.New("aborting after signal"), log.Data{"signal": sig.String()})
			looping = false
		case insertedObservationsErrorMessage := <-observationsInsertedEventConsumer.Errors():
			log.Error(errors.New("aborting after consumer error"), log.Data{"error": insertedObservationsErrorMessage, "topic": cfg.ObservationsInsertedTopic})
			looping = false
		case newImportConsumerErrorMessage := <-newInstanceEventConsumer.Errors():
			log.Error(errors.New("aborting after consumer error"), log.Data{"error": newImportConsumerErrorMessage, "topic": cfg.NewInstanceTopic})
			looping = false
		case <-instanceLoopEndedChan:
			log.Error(errors.New("unexpected instance loop exit"), nil)
			looping = false
		case err = <-httpServerDoneChan:
			log.ErrorC("unexpected httpServer exit", err, nil)
			looping = false
		case newInstanceMessage := <-newInstanceEventConsumer.Incoming():
			var newInstanceEvent inputFileAvailable
			if err := schema.InputFileAvailableSchema.Unmarshal(newInstanceMessage.GetData(), &newInstanceEvent); err != nil {
				log.ErrorC("TODO handle unmarshal error", err, log.Data{"topic": cfg.NewInstanceTopic})
			} else {
				createInstanceChan <- newInstanceEvent.InstanceID
			}
			newInstanceMessage.Commit()
		case insertedMessage := <-observationsInsertedEventConsumer.Incoming():
			var insertedUpdate insertedObservationsEvent
			if err := schema.ObservationsInsertedEvent.Unmarshal(insertedMessage.GetData(), &insertedUpdate); err != nil {
				log.ErrorC("unmarshal error", err, log.Data{"topic": cfg.ObservationsInsertedTopic, "msg": insertedMessage})
			} else {
				insertedUpdate.DoneChan = make(chan bool)
				updateInstanceWithObservationsInsertedChan <- insertedUpdate
				if isFatal := <-insertedUpdate.DoneChan; !isFatal {
					// do not commit, update was non-fatal (will retry)
					continue
				}
			}
			insertedMessage.Commit()
		}
	}

	// XXX XXX XXX XXX
	// XXX assert: XXX only get here when we have an error, which has been logged
	// XXX XXX XXX XXX

	// gracefully shutdown the application, closing any open resources
	log.Error(errors.New("Shutting down with timeout"), log.Data{"timeout": cfg.ShutdownTimeout})
	shutdownContext, shutdownContextCancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)

	waitCount := int32(0)
	waitsLeft := int32(0)
	reduceWaits := make(chan bool)

	// first tell the kafka consumers to stop receiving, wait for instanceLoopEndedChan, then close
	waitsLeft = atomic.AddInt32(&waitCount, 1)
	go func() {
		if err := observationsInsertedEventConsumer.StopListeningToConsumer(shutdownContext); err != nil {
			log.ErrorC("bad listen stop", err, log.Data{"topic": cfg.ObservationsInsertedTopic})
		} else {
			log.Debug("listen stopped", log.Data{"topic": cfg.ObservationsInsertedTopic})
		}
		<-instanceLoopEndedChan
		observationsInsertedEventConsumer.Close(shutdownContext)
		reduceWaits <- true
	}()
	waitsLeft = atomic.AddInt32(&waitCount, 1)
	// repeat above for 2nd consumer
	go func() {
		if err := newInstanceEventConsumer.StopListeningToConsumer(shutdownContext); err != nil {
			log.ErrorC("bad listen stop", err, log.Data{"topic": cfg.NewInstanceTopic})
		} else {
			log.Debug("listen stopped", log.Data{"topic": cfg.NewInstanceTopic})
		}
		// initiate final close of the consumers (and contexts) when instance loop has completed
		<-instanceLoopEndedChan
		newInstanceEventConsumer.Close(shutdownContext)
		reduceWaits <- true
	}()

	// background wait for httpServer to shutdown
	waitsLeft = atomic.AddInt32(&waitCount, 1)
	go func() {
		api.StopHealthCheck(shutdownContext)
		<-httpServerDoneChan
		// time.Sleep(11 * time.Second) // TODO testing
		reduceWaits <- true
	}()

	// background cancel and wait for the instance handler to stop
	waitsLeft = atomic.AddInt32(&waitCount, 1)
	go func() {
		instanceHandlerCancel()
		<-instanceLoopEndedChan
		reduceWaits <- true
	}()

	// loop until context is done (cancelled or timeout) or waitCount==0 (cancels the context)
	for contextRunning := true; contextRunning; {
		select {
		case <-shutdownContext.Done():
			if shutdownContext.Err() != nil {
				log.ErrorC("timed out while shutting down", shutdownContext.Err(), nil)
			}
			contextRunning = false
		case <-reduceWaits:
			if waitsLeft = atomic.AddInt32(&waitCount, -1); waitsLeft == 0 {
				shutdownContextCancel()
			}
		}
	}

	log.Info("Shutdown done", log.Data{"waitsLeft": waitsLeft})
	os.Exit(1)
}
