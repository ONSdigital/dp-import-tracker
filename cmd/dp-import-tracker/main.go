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

	"github.com/ONSdigital/dp-graph/graph"
	"github.com/ONSdigital/dp-import-tracker/api"
	"github.com/ONSdigital/dp-import-tracker/config"
	"github.com/ONSdigital/dp-import/events"
	"github.com/ONSdigital/go-ns/kafka"
	"github.com/ONSdigital/go-ns/rchttp"
	"github.com/ONSdigital/log.go/log"
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
	totalObservations            int64
	observationsInsertedCount    int64
	observationInsertComplete    bool
	buildHierarchyTasks          map[string]buildHierarchyTask
	buildSearchTasks             map[string]buildSearchTask
	jobID                        string
	begunObservationInsertUpdate bool
}

func (instance trackedInstance) AllSearchIndexesAreBuilt() bool {

	for _, task := range instance.buildSearchTasks {
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

// updateInstanceFromDatasetAPI updates a specific import instance with the current counts of expected/complete observations
func (trackedInstances trackedInstanceList) updateInstanceFromDatasetAPI(ctx context.Context, datasetAPI *api.DatasetAPI, instanceID string) (bool, error) {
	instanceFromAPI, isFatal, err := datasetAPI.GetInstance(ctx, instanceID)
	if err != nil {
		return isFatal, err
	}
	if instanceFromAPI.InstanceID == "" {
		log.Event(ctx, "no such instance at API - removing from tracker", log.INFO, log.Data{"InstanceID": instanceID})
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

		for _, task := range instanceFromAPI.ImportTasks.BuildSearchIndexTasks {
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
		for _, task := range instance.ImportTasks.BuildSearchIndexTasks {
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
	for _, instance := range instancesFromAPI {

		hierarchyTasks := createHierarchyTaskMapFromInstance(instance)
		searchTasks := createSearchTaskMapFromInstance(instance)

		log.Event(ctx, "adding instance for tracking", log.INFO, log.Data{
			"instance_id":    instance.InstanceID,
			"current_state":  instance.State,
			"hierarchy_taks": hierarchyTasks,
			"search_tasks":   searchTasks,
		})

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
	log.Event(ctx, "calling import api to update job state", log.INFO, log.Data{"job_id": jobID, "setting_state": targetState})
	if err := importAPI.UpdateImportJobState(ctx, jobID, targetState); err != nil {
		log.Event(ctx, "CheckImportJobCompletionState update", log.ERROR, log.Error(err), log.Data{"jobID": jobID, "last completed instanceID": completedInstanceID})
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
	store *graph.DB,
	dataImportCompleteProducer kafka.Producer,
	checkCompleteInterval time.Duration,
	initialiseListInterval time.Duration,
	initialiseListAttempts int,
) {

	// inform main() when we have stopped processing events
	defer close(instanceLoopDoneChan)

	trackedInstances := make(trackedInstanceList)
	for i := 1; i <= initialiseListAttempts; i++ {
		if _, err := trackedInstances.getInstanceList(ctx, datasetAPI); err != nil {
			if i == initialiseListAttempts {
				log.Event(ctx, "failed to obtain initial instance list", log.ERROR, log.Error(err), log.Data{"attempt": i})
				return
			}
			log.Event(ctx, "could not obtain initial instance list - will retry", log.ERROR, log.Error(err), log.Data{"attempt": i})
			time.Sleep(initialiseListInterval)
			continue
		}
		break
	}

	tickerChan := make(chan bool)
	go func() {
		for range time.Tick(checkCompleteInterval) {
			tickerChan <- true
		}
	}()

	for looping := true; looping; {
		select {
		case <-ctx.Done():
			log.Event(ctx, "manageActiveInstanceEvents: loop ending (context done)", log.INFO)
			looping = false
		case <-tickerChan:
			for instanceID := range trackedInstances {

				stopTracking := false

				logData := log.Data{
					"instance_id": instanceID,
					"job_id":      trackedInstances[instanceID].jobID,
				}

				if isFatal, err := trackedInstances.updateInstanceFromDatasetAPI(ctx, datasetAPI, instanceID); err != nil {
					log.Event(ctx, "could not update instance", log.ERROR, log.Error(err), log.Data{"instanceID": instanceID, "isFatal": isFatal})
					stopTracking = isFatal
				} else if !trackedInstances[instanceID].observationInsertComplete &&
					trackedInstances[instanceID].totalObservations > 0 &&
					trackedInstances[instanceID].observationsInsertedCount >= trackedInstances[instanceID].totalObservations {

					// the above check on totalObservations ensures that we have updated this instance from the Import API (non-default value)
					// and that inserts have exceeded the expected
					logData["observations"] = trackedInstances[instanceID].observationsInsertedCount
					logData["total_observations"] = trackedInstances[instanceID].totalObservations

					log.Event(ctx, "import observations possibly complete - will check db", log.INFO, logData)
					// check db for actual count(insertedObservations) - avoid kafka-double-counting
					countObservations, err := store.CountInsertedObservations(context.Background(), instanceID)
					if err != nil {
						log.Event(ctx, "Failed to check db for actual count(insertedObservations) now instance appears to be completed", log.ERROR, log.Error(err), logData)
					} else {
						logData["db_count"] = countObservations
						if countObservations != trackedInstances[instanceID].totalObservations {
							log.Event(ctx, "db_count of inserted observations != expected total - will continue to monitor", log.INFO, logData)
						} else {

							log.Event(ctx, "import observations complete, calling dataset api to set import observation task complete", log.INFO, logData)
							if isFatal, err := datasetAPI.SetImportObservationTaskComplete(ctx, instanceID); err != nil {
								logData["isFatal"] = isFatal
								log.Event(ctx, "failed to set import observation task state=completed", log.ERROR, log.Error(err), logData)
								stopTracking = isFatal
							}

							err = produceDataImportCompleteEvents(trackedInstances[instanceID], instanceID, dataImportCompleteProducer)
							if err != nil {
								log.Event(ctx, "failed to send data import complete events", log.ERROR, log.Error(err), logData)
								stopTracking = true
							}
						}
					}
				} else if trackedInstances[instanceID].observationInsertComplete &&
					trackedInstances[instanceID].AllSearchIndexesAreBuilt() {

					// assume no error, i.e. that updating works, so we can stopTracking
					stopTracking = true
					if isFatal, err := datasetAPI.UpdateInstanceState(ctx, instanceID, "completed"); err != nil {
						logData["isFatal"] = isFatal
						log.Event(ctx, "failed to set import instance state=completed", log.ERROR, log.Error(err), logData)
						stopTracking = isFatal
					} else if isFatal, err := CheckImportJobCompletionState(ctx, importAPI, datasetAPI, trackedInstances[instanceID].jobID, instanceID); err != nil {
						logData["isFatal"] = isFatal
						log.Event(ctx, "failed to check import job when instance completed", log.ERROR, log.Error(err), logData)
						stopTracking = isFatal
					}
				}

				if stopTracking {
					// no (or fatal) errors, so stop tracking the completed (or failed) instance
					delete(trackedInstances, instanceID)
					log.Event(ctx, "ceased monitoring instance", log.INFO, logData)
				}

			}
		case newInstanceMsg := <-createInstanceChan:
			if _, ok := trackedInstances[newInstanceMsg.InstanceID]; ok {
				log.Event(ctx, "", log.ERROR, log.Error(errors.New("import instance exists"), log.Data{"instanceID": newInstanceMsg}))
				continue
			}

			log.Event(ctx, "new import instance", log.INFO, log.Data{
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
			if ti, ok := trackedInstances[instanceID]; !ok {
				log.Event(ctx, "warning: import instance not in tracked list for update", log.INFO, logData)
			} else {
				if !ti.begunObservationInsertUpdate {
					log.Event(ctx, "updating number of observations inserted for instance", log.INFO, log.Data{"instance_id": instanceID})
					ti.begunObservationInsertUpdate = true
					trackedInstances[instanceID] = ti
				}
			}
			if isFatal, err := datasetAPI.UpdateInstanceWithNewInserts(ctx, instanceID, observationsInserted); err != nil {
				logData["is_fatal"] = isFatal
				log.Event(ctx, "failed to add inserts to instance", log.ERROR, log.Error(err), logData)
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

		log.Event(ctx, "data import complete, producing event message", log.INFO,
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
func logFatal(ctx context.Context, contextMessage string, err error, data log.Data) {
	log.Event(ctx, contextMessage, log.FATAL, log.Error(err), data)
	panic(err)
}

func main() {
	log.Namespace = "dp-import-tracker"

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)

	cfg, err := config.NewConfig()
	if err != nil {
		logFatal(mainContext, "config failed", err, nil)
	}

	// sensitive fields are omitted from config.String().
	log.Event(mainContext, "loaded config", log.INFO, log.Data{"config": cfg})

	httpServerDoneChan := make(chan error)
	api.StartHealthCheck(cfg.BindAddr, httpServerDoneChan)

	newInstanceEventConsumer, err := kafka.NewConsumerGroup(
		cfg.Brokers,
		cfg.NewInstanceTopic,
		cfg.NewInstanceConsumerGroup,
		kafka.OffsetNewest)
	if err != nil {
		logFatal(mainContext, mainContext, "could not obtain consumer", err, log.Data{"topic": cfg.NewInstanceTopic})
	}

	observationsInsertedEventConsumer, err := kafka.NewConsumerGroup(
		cfg.Brokers,
		cfg.ObservationsInsertedTopic,
		cfg.ObservationsInsertedConsumerGroup,
		kafka.OffsetNewest)
	if err != nil {
		logFatal(mainContext, "could not obtain consumer", err, log.Data{"topic": cfg.ObservationsInsertedTopic})
	}

	hierarchyBuiltConsumer, err := kafka.NewConsumerGroup(
		cfg.Brokers,
		cfg.HierarchyBuiltTopic,
		cfg.HierarchyBuiltConsumerGroup,
		kafka.OffsetNewest)
	if err != nil {
		logFatal(mainContext, "could not obtain consumer", err, log.Data{"topic": cfg.HierarchyBuiltTopic})
	}

	searchBuiltConsumer, err := kafka.NewConsumerGroup(
		cfg.Brokers,
		cfg.SearchBuiltTopic,
		cfg.SearchBuiltConsumerGroup,
		kafka.OffsetNewest)
	if err != nil {
		logFatal(mainContext, "could not obtain consumer", err, log.Data{"topic": cfg.SearchBuiltTopic})
	}

	dataImportCompleteProducer, err := kafka.NewProducer(cfg.Brokers, cfg.DataImportCompleteTopic, 0)
	if err != nil {
		logFatal(mainContext, "observation import complete kafka producer error", err, nil)
	}

	graphDB, err := graph.NewInstanceStore(context.Background())
	if err != nil {
		logFatal(mainContext, "could not obtain database connection", err, nil)
	}

	client := rchttp.DefaultClient
	client.MaxRetries = 4

	importAPI := api.NewImportAPI(client, cfg.ImportAPIAddr, cfg.ServiceAuthToken)
	datasetAPI := api.NewDatasetAPI(client, cfg.DatasetAPIAddr, cfg.ServiceAuthToken)

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
		graphDB,
		dataImportCompleteProducer,
		cfg.CheckCompleteInterval,
		cfg.InitialiseListInterval,
		cfg.InitialiseListAttempts,
	)

	// loop over consumers messages and errors - in background, so we can attempt graceful shutdown
	// sends instance events to the (above) instance event handler
	mainLoopEndedChan := make(chan error)
	go func() {
		defer close(mainLoopEndedChan)
		var err error

		for looping := true; looping; {
			select {
			case <-mainContext.Done():
				log.Event(mainContext, "main loop aborting", log.INFO, log.Data{"ctx_err": mainContext.Err()})
				looping = false
			case err = <-observationsInsertedEventConsumer.Errors():
				log.Event(mainContext, "aborting after consumer error", log.ERROR, log.Error(err), log.Data{"topic": cfg.ObservationsInsertedTopic})
				looping = false
			case err = <-newInstanceEventConsumer.Errors():
				log.Event(mainContext, "aborting after consumer error", log.ERROR, log.Error(err), log.Data{"topic": cfg.NewInstanceTopic})
				looping = false
			case err = <-httpServerDoneChan:
				log.Event(mainContext, "unexpected httpServer exit", log.ERROR, log.Error(err), nil)
				looping = false
			case newInstanceMessage := <-newInstanceEventConsumer.Incoming():
				var newInstanceEvent events.InputFileAvailable
				if err = events.InputFileAvailableSchema.Unmarshal(newInstanceMessage.GetData(), &newInstanceEvent); err != nil {
					log.Event(mainContext, "TODO handle unmarshal error", log.ERROR, log.Error(err), log.Data{"topic": cfg.NewInstanceTopic})
				} else {
					createInstanceChan <- newInstanceEvent
				}
				newInstanceMessage.Commit()
			case insertedMessage := <-observationsInsertedEventConsumer.Incoming():
				var insertedUpdate insertedObservationsEvent
				msg := insertedMessage.GetData()
				if err = events.ObservationsInsertedSchema.Unmarshal(msg, &insertedUpdate); err != nil {
					log.Event(mainContext, "unmarshal error", log.ERROR, log.Error(err), log.Data{"topic": cfg.ObservationsInsertedTopic, "msg": string(msg)})
				} else {
					insertedUpdate.DoneChan = make(chan insertResult)
					updateInstanceWithObservationsInsertedChan <- insertedUpdate
					if insertRes := <-insertedUpdate.DoneChan; insertRes == ErrRetry {
						// do not commit, update was non-fatal (will retry)
						log.Event(mainContext, "non-commit", log.ERROR, log.Error(errors.New("non-fatal error"), log.Data{"topic": cfg.ObservationsInsertedTopic, "msg": string(msg)}))
						continue
					}
				}
				insertedMessage.Commit()
			case hierarchyBuiltMessage := <-hierarchyBuiltConsumer.Incoming():
				var event events.HierarchyBuilt
				if err = events.HierarchyBuiltSchema.Unmarshal(hierarchyBuiltMessage.GetData(), &event); err != nil {

					// todo: call error reporter
					log.Event(mainContext, "unmarshal error", log.ERROR, log.Error(err), log.Data{"topic": cfg.HierarchyBuiltTopic})

				} else {

					logData := log.Data{"instance_id": event.InstanceID, "dimension_name": event.DimensionName}
					log.Event(mainContext, "processing hierarchy built message", log.INFO, logData)

					if isFatal, err := datasetAPI.UpdateInstanceWithHierarchyBuilt(
						mainContext,
						event.InstanceID,
						event.DimensionName); err != nil {

						if isFatal {
							// todo: call error reporter
							log.Event(mainContext, "failed to update instance with hierarchy built status", log.ERROR, log.Error(err), logData)
						}
					}
					log.Event(mainContext, "updated instance with hierarchy built. committing message", log.INFO, logData)
				}

				hierarchyBuiltMessage.Commit()

			case searchBuiltMessage := <-searchBuiltConsumer.Incoming():
				var event events.SearchIndexBuilt
				if err = events.SearchIndexBuiltSchema.Unmarshal(searchBuiltMessage.GetData(), &event); err != nil {

					// todo: call error reporter
					log.Event(mainContext, "unmarshal error", log.ERROR, log.Error(err), log.Data{"topic": cfg.HierarchyBuiltTopic})

				} else {

					logData := log.Data{"instance_id": event.InstanceID, "dimension_name": event.DimensionName}
					log.Event(mainContext, "processing search index built message", log.INFO, logData)

					if isFatal, err := datasetAPI.UpdateInstanceWithSearchIndexBuilt(
						mainContext,
						event.InstanceID,
						event.DimensionName); err != nil {

						if isFatal {
							// todo: call error reporter
							log.Event(mainContext, "failed to update instance with hierarchy built status", log.ERROR, log.Error(err), logData)
						}
					}
					log.Event(mainContext, "updated instance with search index built. committing message", log.INFO, logData)
				}

				searchBuiltMessage.Commit()
			}

		}
		log.Event(mainContext, "main loop completed", log.INFO)
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
	log.Event(mainContext, "Start shutdown", log.ERROR, log.Error(err), log.Data{"timeout": cfg.ShutdownTimeout})
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
			log.Event(mainContext, "bad listen stop", log.ERROR, log.Error(err), log.Data{"topic": cfg.ObservationsInsertedTopic})
		} else {
			log.Event(mainContext, "listen stopped", log.INFO, log.Data{"topic": cfg.ObservationsInsertedTopic})
		}
		<-mainLoopEndedChan
		if err = observationsInsertedEventConsumer.Close(shutdownContext); err != nil {
			log.Event(mainContext, "bad close", log.ERROR, log.Error(err), log.Data{"topic": cfg.ObservationsInsertedTopic})
		}
	})
	// repeat above for 2nd consumer
	backgroundAndCount(&waitCount, reduceWaits, func() {
		var err error
		if err = newInstanceEventConsumer.StopListeningToConsumer(shutdownContext); err != nil {
			log.Event(mainContext, "bad listen stop", log.ERROR, log.Error(err), log.Data{"topic": cfg.NewInstanceTopic})
		} else {
			log.Event(mainContext, "listen stopped", log.INFO, log.Data{"topic": cfg.NewInstanceTopic})
		}
		<-mainLoopEndedChan
		if err = newInstanceEventConsumer.Close(shutdownContext); err != nil {
			log.Event(mainContext, "bad close", log.ERROR, log.Errro(err), log.Data{"topic": cfg.NewInstanceTopic})
		}
		if err = graphDB.Close(shutdownContext); err != nil {
			log.Event(mainContext, "bad db close", log.ERROR, log.Error(err), nil)
		}
	})

	backgroundAndCount(&waitCount, reduceWaits, func() {
		var err error
		if err = hierarchyBuiltConsumer.StopListeningToConsumer(shutdownContext); err != nil {
			log.Event(mainContext, "bad listen stop", log.ERROR, log.Error(err), log.Data{"topic": cfg.HierarchyBuiltTopic})
		} else {
			log.Event(mainContext, "listen stopped", log.INFO, log.Data{"topic": cfg.HierarchyBuiltTopic})
		}
		<-mainLoopEndedChan
		if err = hierarchyBuiltConsumer.Close(shutdownContext); err != nil {
			log.Event(mainContext, "bad close", log.ERROR, log.Error(err), log.Data{"topic": cfg.HierarchyBuiltTopic})
		}
	})

	backgroundAndCount(&waitCount, reduceWaits, func() {
		var err error
		if err = searchBuiltConsumer.StopListeningToConsumer(shutdownContext); err != nil {
			log.Event(mainContext, "bad listen stop", log.ERROR, log.Error(err), log.Data{"topic": cfg.SearchBuiltTopic})
		} else {
			log.Event(mainContext, "listen stopped", log.INFO, log.Data{"topic": cfg.SearchBuiltTopic})
		}
		<-mainLoopEndedChan
		if err = searchBuiltConsumer.Close(shutdownContext); err != nil {
			log.Event(mainContext, "bad close", log.ERROR, log.Error(err), log.Data{"topic": cfg.SearchBuiltTopic})
		}
	})

	// background httpServer shutdown
	backgroundAndCount(&waitCount, reduceWaits, func() {
		var err error
		if err = api.StopHealthCheck(shutdownContext); err != nil {
			log.Event(mainContext, "bad healthcheck close", log.ERROR, log.Error(err))
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

	log.Event(mainContext, "Shutdown done", log.INFO, log.Data{"context": shutdownContext.Err(), "waits_left": waitCount})
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
