package main

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ONSdigital/dp-import-tracker/api"
	"github.com/ONSdigital/dp-import-tracker/config"
	"github.com/ONSdigital/dp-import-tracker/store"
	"github.com/ONSdigital/dp-import/events"
	"github.com/ONSdigital/go-ns/kafka"
	"github.com/ONSdigital/go-ns/log"
	"github.com/ONSdigital/go-ns/rchttp"
)

type insertResult int

const (
	// this zero value reserved for positive outcome (default)
	// AllGood  insertResult = 0

	// ErrPerm is a permanent/fatal error
	ErrPerm insertResult = 1
	// ErrRetry indicates a retriable error
	ErrRetry insertResult = 2
)

type insertedObservationsEvent struct {
	InstanceID                   string            `json:"instance_id" avro:"instance_id"`
	NumberOfObservationsInserted int32             `json:"number_of_inserted_observations" avro:"observations_inserted"`
	DoneChan                     chan insertResult `json:"-" avro:"-"`
}

type trackedInstance struct {
	totalObservations         int64
	observationsInsertedCount int64
	observationInsertComplete bool
	buildHierarchyTasks       map[string]buildHierarchyTask
	buildSearchTasks          map[string]buildSearchTask
	jobID                     string
}

func (instance trackedInstance) AllHierarchiesAreImported() bool {

	for _, task := range instance.buildHierarchyTasks {
		if !task.isComplete {
			return false
		}
	}

	return true
}

// buildHierarchyTask represents a task of importing a single hierarchy.
type buildHierarchyTask struct {
	isComplete    bool
	dimensionName string
	codeListID    string
}

type buildSearchTask struct {
	isComplete    bool
	dimensionName string
}

type trackedInstanceList map[string]trackedInstance

var checkForCompleteInstancesTick = time.Millisecond * 2000

// updateInstanceFromDatasetAPI updates a specific import instance with the current counts of expected/complete observations
func (trackedInstances trackedInstanceList) updateInstanceFromDatasetAPI(ctx context.Context, datasetAPI *api.DatasetAPI, instanceID string) (bool, error) {
	instanceFromAPI, isFatal, err := datasetAPI.GetInstance(ctx, instanceID)
	if err != nil {
		return isFatal, err
	}
	if instanceFromAPI.InstanceID == "" {
		log.Debug("no such instance at API - removing from tracker", log.Data{"InstanceID": instanceID})
		delete(trackedInstances, instanceID)
		return false, nil
	}

	tempCopy := trackedInstances[instanceID]

	if obsCount := instanceFromAPI.NumberOfObservations; obsCount > 0 && obsCount != trackedInstances[instanceID].totalObservations {
		tempCopy.totalObservations = obsCount
	}
	if observationsInsertedCount := instanceFromAPI.ImportTasks.ImportObservations.InsertedObservations; observationsInsertedCount > 0 && observationsInsertedCount != trackedInstances[instanceID].observationsInsertedCount {
		tempCopy.observationsInsertedCount = observationsInsertedCount
	}

	tempCopy.observationInsertComplete = instanceFromAPI.ImportTasks.ImportObservations.State == "completed"

	if tempCopy.buildHierarchyTasks == nil {
		hierarchyTasks := createHierarchyTaskMapFromInstance(instanceFromAPI)
		tempCopy.buildHierarchyTasks = hierarchyTasks

		searchTasks := createSearchTaskMapFromInstance(instanceFromAPI)
		tempCopy.buildSearchTasks = searchTasks
	} else {
		for _, task := range instanceFromAPI.ImportTasks.BuildHierarchyTasks {
			hierarchyTask := tempCopy.buildHierarchyTasks[task.CodeListID]
			hierarchyTask.isComplete = task.State == "completed"
			tempCopy.buildHierarchyTasks[task.CodeListID] = hierarchyTask
		}

		for _, task := range instanceFromAPI.ImportTasks.BuildSearchTasks {
			searchTask := tempCopy.buildSearchTasks[task.DimensionName]
			searchTask.isComplete = task.State == "completed"
			tempCopy.buildSearchTasks[task.DimensionName] = searchTask
		}
	}

	trackedInstances[instanceID] = tempCopy
	return false, nil
}

func createHierarchyTaskMapFromInstance(instance api.Instance) map[string]buildHierarchyTask {

	buildHierarchyTasks := make(map[string]buildHierarchyTask, 0)

	if instance.ImportTasks != nil {
		for _, task := range instance.ImportTasks.BuildHierarchyTasks {
			buildHierarchyTasks[task.CodeListID] = buildHierarchyTask{
				isComplete:    task.State == "completed",
				dimensionName: task.DimensionName,
				codeListID:    task.CodeListID,
			}
		}
	}

	return buildHierarchyTasks
}

func createSearchTaskMapFromInstance(instance api.Instance) map[string]buildSearchTask {

	buildSearchTasks := make(map[string]buildSearchTask, 0)

	if instance.ImportTasks != nil {
		for _, task := range instance.ImportTasks.BuildSearchTasks {
			buildSearchTasks[task.DimensionName] = buildSearchTask{
				isComplete:    task.State == "completed",
				dimensionName: task.DimensionName,
			}
		}
	}

	return buildSearchTasks
}

// getInstanceList gets a list of current import Instances, to seed our in-memory list
func (trackedInstances trackedInstanceList) getInstanceList(ctx context.Context, api *api.DatasetAPI) (bool, error) {
	instancesFromAPI, isFatal, err := api.GetInstances(ctx, url.Values{"state": []string{"submitted"}})
	if err != nil {
		return isFatal, err
	}
	log.Debug("instances", log.Data{"api": instancesFromAPI})
	for _, instance := range instancesFromAPI {

		hierarchyTasks := createHierarchyTaskMapFromInstance(instance)
		searchTasks := createSearchTaskMapFromInstance(instance)

		if instance.ImportTasks != nil {
			trackedInstances[instance.InstanceID] = trackedInstance{
				totalObservations:         instance.NumberOfObservations,
				observationsInsertedCount: instance.ImportTasks.ImportObservations.InsertedObservations,
				observationInsertComplete: instance.ImportTasks.ImportObservations.State == "completed",
				jobID:                     instance.Links.Job.ID,
				buildHierarchyTasks:       hierarchyTasks,
				buildSearchTasks:          searchTasks,
			}
		}

	}
	return false, nil
}

// CheckImportJobCompletionState checks all instances for given import job - if all completed, mark import as completed
func CheckImportJobCompletionState(ctx context.Context, importAPI *api.ImportAPI, datasetAPI *api.DatasetAPI, jobID, completedInstanceID string) (bool, error) {
	importJobFromAPI, isFatal, err := importAPI.GetImportJob(ctx, jobID)
	if err != nil {
		return isFatal, err
	}
	// check for 404 not found (empty importJobFromAPI)
	if importJobFromAPI.JobID == "" {
		return false, errors.New("CheckImportJobCompletionState ImportAPI did not recognise jobID")
	}

	targetState := "completed"
	log.Debug("checking", log.Data{"API insts": importJobFromAPI.Links.Instances})
	for _, instanceRef := range importJobFromAPI.Links.Instances {
		if instanceRef.ID == completedInstanceID {
			continue
		}
		// XXX TODO code below largely untested, and possibly subject to race conditions
		instanceFromAPI, isFatal, err := datasetAPI.GetInstance(ctx, instanceRef.ID)
		if err != nil {
			return isFatal, err
		}
		if instanceFromAPI.State != "completed" && instanceFromAPI.State != "error" {
			return false, nil
		}
		if instanceFromAPI.State == "error" {
			targetState = instanceFromAPI.State
		}
	}
	// assert: all instances for jobID are marked "completed"/"error", so update import as same
	if err := importAPI.UpdateImportJobState(ctx, jobID, targetState); err != nil {
		log.ErrorC("CheckImportJobCompletionState update", err, log.Data{"jobID": jobID, "last completed instanceID": completedInstanceID})
	}
	return false, nil
}

// manageActiveInstanceEvents handles all updates to trackedInstances in one thread (this is only called once, in its own thread)
func manageActiveInstanceEvents(
	ctx context.Context,
	createInstanceChan chan events.InputFileAvailable,
	updateInstanceWithObservationsInsertedChan chan insertedObservationsEvent,
	datasetAPI *api.DatasetAPI,
	importAPI *api.ImportAPI,
	instanceLoopDoneChan chan bool,
	store store.Storer,
	dataImportCompleteProducer kafka.Producer,
) {

	// inform main() when we have stopped processing events
	defer close(instanceLoopDoneChan)

	trackedInstances := make(trackedInstanceList)
	if _, err := trackedInstances.getInstanceList(ctx, datasetAPI); err != nil {
		logFatal("could not obtain initial instance list", err, nil)
	}

	tickerChan := make(chan bool)
	go func() {
		for range time.Tick(checkForCompleteInstancesTick) {
			tickerChan <- true
		}
	}()

	for looping := true; looping; {
		select {
		case <-ctx.Done():
			log.Debug("manageActiveInstanceEvents: loop ending (context done)", nil)
			looping = false
		case <-tickerChan:
			log.Debug("check import Instances", log.Data{"q": trackedInstances})
			for instanceID := range trackedInstances {

				stopTracking := false

				logData := log.Data{
					"instance_id": instanceID,
					"job_id":      trackedInstances[instanceID].jobID,
				}

				if isFatal, err := trackedInstances.updateInstanceFromDatasetAPI(ctx, datasetAPI, instanceID); err != nil {
					log.ErrorC("could not update instance", err, log.Data{"instanceID": instanceID, "isFatal": isFatal})

				} else if !trackedInstances[instanceID].observationInsertComplete &&
					trackedInstances[instanceID].totalObservations > 0 &&
					trackedInstances[instanceID].observationsInsertedCount >= trackedInstances[instanceID].totalObservations {

					// the above check on totalObservations ensures that we have updated this instance from the Import API (non-default value)
					// and that inserts have exceeded the expected
					logData["observations"] = trackedInstances[instanceID].observationsInsertedCount
					logData["total_observations"] = trackedInstances[instanceID].totalObservations

					log.Debug("import observations possibly complete - will check db", logData)
					// check db for actual count(insertedObservations) - avoid kafka-double-counting
					countObservations, err := store.CountInsertedObservations(instanceID)
					if err != nil {
						log.ErrorC("Failed to check db for actual count(insertedObservations) now instance appears to be completed", err, logData)
					} else {
						logData["db_count"] = countObservations
						if countObservations != trackedInstances[instanceID].totalObservations {
							log.Trace("db_count of inserted observations != expected total - will continue to monitor", logData)
						} else {

							log.Debug("import observations complete, calling dataset api to set import observation task complete", logData)
							if isFatal, err := datasetAPI.SetImportObservationTaskComplete(ctx, instanceID); err != nil {
								logData["isFatal"] = isFatal
								log.ErrorC("failed to set import observation task state=completed", err, logData)
								stopTracking = isFatal
							}

							err = produceDataImportCompleteEvents(trackedInstances[instanceID], instanceID, dataImportCompleteProducer)
							if err != nil {
								log.ErrorC("failed to send data import complete events", err, logData)
								stopTracking = true
							}
						}
					}
				} else if trackedInstances[instanceID].observationInsertComplete &&
					trackedInstances[instanceID].AllHierarchiesAreImported() {

					// assume no error, i.e. that updating works, so we can stopTracking
					stopTracking = true
					if isFatal, err := datasetAPI.UpdateInstanceState(ctx, instanceID, "completed"); err != nil {
						logData["isFatal"] = isFatal
						log.ErrorC("failed to set import instance state=completed", err, logData)
						stopTracking = isFatal
					} else if isFatal, err := CheckImportJobCompletionState(ctx, importAPI, datasetAPI, trackedInstances[instanceID].jobID, instanceID); err != nil {
						logData["isFatal"] = isFatal
						log.ErrorC("failed to check import job when instance completed", err, logData)
						stopTracking = isFatal
					}
				}

				if stopTracking {
					// no (or fatal) errors, so stop tracking the completed (or failed) instance
					delete(trackedInstances, instanceID)
					log.Trace("ceased monitoring instance", logData)
				}

			}
		case newInstanceMsg := <-createInstanceChan:
			if _, ok := trackedInstances[newInstanceMsg.InstanceID]; ok {
				log.Error(errors.New("import instance exists"), log.Data{"instanceID": newInstanceMsg})
				continue
			}

			log.Debug("new import instance", log.Data{
				"instance_id": newInstanceMsg.InstanceID,
				"job_id":      newInstanceMsg.JobID,
				"url":         newInstanceMsg.URL,
			})

			trackedInstances[newInstanceMsg.InstanceID] = trackedInstance{
				totalObservations:         -1,
				observationsInsertedCount: 0,
				jobID:                     newInstanceMsg.JobID,
			}

		case updateObservationsInserted := <-updateInstanceWithObservationsInsertedChan:
			instanceID := updateObservationsInserted.InstanceID
			observationsInserted := updateObservationsInserted.NumberOfObservationsInserted
			logData := log.Data{"instance_id": instanceID, "observations_inserted": observationsInserted}
			if _, ok := trackedInstances[instanceID]; !ok {
				log.Info("warning: import instance not in tracked list for update", logData)
			}
			log.Debug("updating import instance", logData)
			if isFatal, err := datasetAPI.UpdateInstanceWithNewInserts(ctx, instanceID, observationsInserted); err != nil {
				logData["is_fatal"] = isFatal
				log.ErrorC("failed to add inserts to instance", err, logData)
				if isFatal {
					updateObservationsInserted.DoneChan <- ErrPerm
				} else {
					updateObservationsInserted.DoneChan <- ErrRetry
				}
			} else {
				close(updateObservationsInserted.DoneChan)
			}
		}
	}
	log.Info("Instance loop completed", nil)
}

func produceDataImportCompleteEvents(
	instance trackedInstance,
	instanceID string,
	dataImportCompleteProducer kafka.Producer) error {

	for _, task := range instance.buildHierarchyTasks {

		dataImportCompleteEvent := &events.DataImportComplete{
			InstanceID:    instanceID,
			CodeListID:    task.codeListID,
			DimensionName: task.dimensionName,
		}

		log.Debug("data import complete, producing event message",
			log.Data{"event": dataImportCompleteEvent})

		bytes, err := events.DataImportCompleteSchema.Marshal(dataImportCompleteEvent)
		if err != nil {
			return err
		}

		dataImportCompleteProducer.Output() <- bytes
	}

	return nil
}

// logFatal is a utility method for a common failure pattern in main()
func logFatal(contextMessage string, err error, data log.Data) {
	log.ErrorC(contextMessage, err, data)
	panic(err)
}

func main() {
	log.Namespace = "dp-import-tracker"

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)

	cfg, err := config.NewConfig()
	if err != nil {
		logFatal("config failed", err, nil)
	}

	// sensitive fields are omitted from config.String().
	log.Debug("loaded config", log.Data{"config": cfg})

	httpServerDoneChan := make(chan error)
	api.StartHealthCheck(cfg.BindAddr, httpServerDoneChan)

	newInstanceEventConsumer, err := kafka.NewConsumerGroup(
		cfg.Brokers,
		cfg.NewInstanceTopic,
		cfg.NewInstanceConsumerGroup,
		kafka.OffsetNewest)
	if err != nil {
		logFatal("could not obtain consumer", err, log.Data{"topic": cfg.NewInstanceTopic})
	}

	observationsInsertedEventConsumer, err := kafka.NewConsumerGroup(
		cfg.Brokers,
		cfg.ObservationsInsertedTopic,
		cfg.ObservationsInsertedConsumerGroup,
		kafka.OffsetNewest)
	if err != nil {
		logFatal("could not obtain consumer", err, log.Data{"topic": cfg.ObservationsInsertedTopic})
	}

	hierarchyBuiltConsumer, err := kafka.NewConsumerGroup(
		cfg.Brokers,
		cfg.HierarchyBuiltTopic,
		cfg.HierarchyBuiltConsumerGroup,
		kafka.OffsetNewest)
	if err != nil {
		logFatal("could not obtain consumer", err, log.Data{"topic": cfg.HierarchyBuiltTopic})
	}

	dataImportCompleteProducer, err := kafka.NewProducer(cfg.Brokers, cfg.DataImportCompleteTopic, 0)
	if err != nil {
		logFatal("observation import complete kafka producer error", err, nil)
	}

	store, err := store.New(cfg.DatabaseAddress, cfg.DatabasePoolSize)
	if err != nil {
		logFatal("could not obtain database connection", err, nil)
	}

	client := rchttp.DefaultClient
	client.MaxRetries = 4

	importAPI := api.NewImportAPI(client, cfg.ImportAPIAddr, cfg.ImportAPIAuthToken)
	datasetAPI := api.NewDatasetAPI(client, cfg.DatasetAPIAddr, cfg.DatasetAPIAuthToken)

	// create context for all work
	mainContext, mainContextCancel := context.WithCancel(context.Background())

	updateInstanceWithObservationsInsertedChan := make(chan insertedObservationsEvent)
	createInstanceChan := make(chan events.InputFileAvailable)
	instanceLoopEndedChan := make(chan bool)

	go manageActiveInstanceEvents(
		mainContext,
		createInstanceChan,
		updateInstanceWithObservationsInsertedChan,
		datasetAPI,
		importAPI,
		instanceLoopEndedChan,
		store,
		dataImportCompleteProducer)

	// loop over consumers messages and errors - in background, so we can attempt graceful shutdown
	// sends instance events to the (above) instance event handler
	mainLoopEndedChan := make(chan error)
	go func() {
		defer close(mainLoopEndedChan)
		var err error

		for looping := true; looping; {
			select {
			case <-mainContext.Done():
				log.Debug("main loop aborting", log.Data{"ctx_err": mainContext.Err()})
				looping = false
			case err = <-observationsInsertedEventConsumer.Errors():
				log.ErrorC("aborting after consumer error", err, log.Data{"topic": cfg.ObservationsInsertedTopic})
				looping = false
			case err = <-newInstanceEventConsumer.Errors():
				log.ErrorC("aborting after consumer error", err, log.Data{"topic": cfg.NewInstanceTopic})
				looping = false
			case err = <-httpServerDoneChan:
				log.ErrorC("unexpected httpServer exit", err, nil)
				looping = false
			case newInstanceMessage := <-newInstanceEventConsumer.Incoming():

				log.Debug("message received on new instance channel", log.Data{
					"offset":newInstanceMessage.Offset(), "message":newInstanceMessage })

				var newInstanceEvent events.InputFileAvailable
				if err = events.InputFileAvailableSchema.Unmarshal(newInstanceMessage.GetData(), &newInstanceEvent); err != nil {
					log.ErrorC("TODO handle unmarshal error", err, log.Data{"topic": cfg.NewInstanceTopic})
				} else {
					createInstanceChan <- newInstanceEvent
				}
				newInstanceMessage.Commit()
			case insertedMessage := <-observationsInsertedEventConsumer.Incoming():


				log.Debug("message received on observation inserted channel", log.Data{
					"offset":insertedMessage.Offset(), "message":insertedMessage })


				var insertedUpdate insertedObservationsEvent
				msg := insertedMessage.GetData()
				if err = events.ObservationsInsertedSchema.Unmarshal(msg, &insertedUpdate); err != nil {
					log.ErrorC("unmarshal error", err, log.Data{"topic": cfg.ObservationsInsertedTopic, "msg": string(msg)})
				} else {
					insertedUpdate.DoneChan = make(chan insertResult)
					updateInstanceWithObservationsInsertedChan <- insertedUpdate
					if insertRes := <-insertedUpdate.DoneChan; insertRes == ErrRetry {
						// do not commit, update was non-fatal (will retry)
						log.ErrorC("non-commit", errors.New("non-fatal error"), log.Data{"topic": cfg.ObservationsInsertedTopic, "msg": string(msg)})
						continue
					}
				}
				insertedMessage.Commit()
			case hierarchyBuiltMessage := <-hierarchyBuiltConsumer.Incoming():
				var event events.HierarchyBuilt
				if err = events.HierarchyBuiltSchema.Unmarshal(hierarchyBuiltMessage.GetData(), &event); err != nil {

					// todo: call error reporter
					log.ErrorC("unmarshal error", err, log.Data{"topic": cfg.HierarchyBuiltTopic})

				} else {

					logData := log.Data{"instance_id": event.InstanceID, "dimension_name": event.DimensionName}
					log.Debug("processing hierarchy built message", logData)

					if isFatal, err := datasetAPI.UpdateInstanceWithHierarchyBuilt(
						mainContext,
						event.InstanceID,
						event.DimensionName); err != nil {

						if isFatal {
							// todo: call error reporter
							log.ErrorC("failed to update instance with hierarchy built status", err, logData)
						}
					}
					log.Debug("updated instance with hierarchy built. committing message", logData)
				}

				hierarchyBuiltMessage.Commit()
			}
		}
		log.Info("main loop completed", nil)
	}()

	// block until signal or fatal errors in a service handler, then continue to shutdown
	select {
	case sig := <-signals:
		err = fmt.Errorf("aborting after signal %q", sig.String())
	case <-mainLoopEndedChan:
		err = errors.New("unexpected main loop exit")
	case <-instanceLoopEndedChan:
		err = errors.New("unexpected instance loop exit")
	}

	// XXX XXX XXX XXX
	// XXX assert: XXX we only get here after we have had a fatal error (err)
	// XXX XXX XXX XXX

	// gracefully shutdown the application, closing any open resources
	log.ErrorC("Start shutdown", err, log.Data{"timeout": cfg.ShutdownTimeout})
	shutdownContext, shutdownContextCancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)

	// tell main handler to stop
	// also tells any in-flight work (with this context) to stop
	mainContextCancel()

	// count background tasks for shutting down services
	waitCount := 0
	reduceWaits := make(chan bool)

	// tell the kafka consumers to stop receiving new messages, wait for mainLoopEndedChan, then consumers.Close
	backgroundAndCount(&waitCount, reduceWaits, func() {
		var err error
		if err = observationsInsertedEventConsumer.StopListeningToConsumer(shutdownContext); err != nil {
			log.ErrorC("bad listen stop", err, log.Data{"topic": cfg.ObservationsInsertedTopic})
		} else {
			log.Debug("listen stopped", log.Data{"topic": cfg.ObservationsInsertedTopic})
		}
		<-mainLoopEndedChan
		if err = observationsInsertedEventConsumer.Close(shutdownContext); err != nil {
			log.ErrorC("bad close", err, log.Data{"topic": cfg.ObservationsInsertedTopic})
		}
	})
	// repeat above for 2nd consumer
	backgroundAndCount(&waitCount, reduceWaits, func() {
		var err error
		if err = newInstanceEventConsumer.StopListeningToConsumer(shutdownContext); err != nil {
			log.ErrorC("bad listen stop", err, log.Data{"topic": cfg.NewInstanceTopic})
		} else {
			log.Debug("listen stopped", log.Data{"topic": cfg.NewInstanceTopic})
		}
		<-mainLoopEndedChan
		if err = newInstanceEventConsumer.Close(shutdownContext); err != nil {
			log.ErrorC("bad close", err, log.Data{"topic": cfg.NewInstanceTopic})
		}
		if err = store.Close(shutdownContext); err != nil {
			log.ErrorC("bad db close", err, nil)
		}
	})

	backgroundAndCount(&waitCount, reduceWaits, func() {
		var err error
		if err = hierarchyBuiltConsumer.StopListeningToConsumer(shutdownContext); err != nil {
			log.ErrorC("bad listen stop", err, log.Data{"topic": cfg.HierarchyBuiltTopic})
		} else {
			log.Debug("listen stopped", log.Data{"topic": cfg.HierarchyBuiltTopic})
		}
		<-mainLoopEndedChan
		if err = hierarchyBuiltConsumer.Close(shutdownContext); err != nil {
			log.ErrorC("bad close", err, log.Data{"topic": cfg.HierarchyBuiltTopic})
		}
	})

	// background httpServer shutdown
	backgroundAndCount(&waitCount, reduceWaits, func() {
		var err error
		if err = api.StopHealthCheck(shutdownContext); err != nil {
			log.ErrorC("bad healthcheck close", err, nil)
		}
		<-httpServerDoneChan
	})

	// loop until context is done (cancelled or timeout) NOTE: waitCount==0 cancels the context
	for contextRunning := true; contextRunning; {
		select {
		case <-shutdownContext.Done():
			contextRunning = false
		case <-reduceWaits:
			waitCount--
			if waitCount == 0 {
				shutdownContextCancel()
			}
		}
	}

	log.Info("Shutdown done", log.Data{"context": shutdownContext.Err(), "waits_left": waitCount})
	os.Exit(1)
}

// backgroundAndCount runs a function in a goroutine, after adding 1 to the waitCount
// - when the function completes, the waitCount is reduced (via a channel message)
func backgroundAndCount(waitCountPtr *int, reduceWaitsChan chan bool, bgFunc func()) {
	*waitCountPtr++
	go func() {
		bgFunc()
		reduceWaitsChan <- true
	}()
}
