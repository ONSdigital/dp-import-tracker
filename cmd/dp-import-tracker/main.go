package main

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/ONSdigital/dp-api-clients-go/v2/dataset"
	"github.com/ONSdigital/dp-api-clients-go/v2/importapi"
	"github.com/ONSdigital/dp-graph/v2/graph"
	"github.com/ONSdigital/dp-healthcheck/healthcheck"
	"github.com/ONSdigital/dp-import-tracker/api"
	"github.com/ONSdigital/dp-import-tracker/config"
	"github.com/ONSdigital/dp-import/events"
	kafka "github.com/ONSdigital/dp-kafka/v2"
	"github.com/ONSdigital/log.go/log"
)

var (
	// BuildTime represents the time in which the service was built
	BuildTime string
	// GitCommit represents the commit (SHA-1) hash of the service that is running
	GitCommit string
	// Version represents the version of the service that is running
	Version string
)

var bufferSize = 1

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
	if instanceFromAPI.ID == "" {
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

	tempCopy.observationInsertComplete = instanceFromAPI.ImportTasks.ImportObservations.State == dataset.StateCompleted.String()

	if tempCopy.buildHierarchyTasks == nil {
		hierarchyTasks := createHierarchyTaskMapFromInstance(instanceFromAPI)
		tempCopy.buildHierarchyTasks = hierarchyTasks

		searchTasks := createSearchTaskMapFromInstance(instanceFromAPI)
		tempCopy.buildSearchTasks = searchTasks
	} else {
		for _, task := range instanceFromAPI.ImportTasks.BuildHierarchyTasks {
			hierarchyTask := tempCopy.buildHierarchyTasks[task.CodeListID]
			hierarchyTask.isComplete = task.State == dataset.StateCompleted.String()
			tempCopy.buildHierarchyTasks[task.CodeListID] = hierarchyTask
		}

		for _, task := range instanceFromAPI.ImportTasks.BuildSearchIndexTasks {
			searchTask := tempCopy.buildSearchTasks[task.DimensionName]
			searchTask.isComplete = task.State == dataset.StateCompleted.String()
			tempCopy.buildSearchTasks[task.DimensionName] = searchTask
		}
	}

	trackedInstances[instanceID] = tempCopy
	return false, nil
}

func createHierarchyTaskMapFromInstance(instance dataset.Instance) map[string]buildHierarchyTask {

	buildHierarchyTasks := make(map[string]buildHierarchyTask, 0)

	if instance.ImportTasks != nil {
		for _, task := range instance.ImportTasks.BuildHierarchyTasks {
			buildHierarchyTasks[task.CodeListID] = buildHierarchyTask{
				isComplete:    task.State == dataset.StateCompleted.String(),
				dimensionName: task.DimensionName,
				codeListID:    task.CodeListID,
			}
		}
	}

	return buildHierarchyTasks
}

func createSearchTaskMapFromInstance(instance dataset.Instance) map[string]buildSearchTask {

	buildSearchTasks := make(map[string]buildSearchTask, 0)

	if instance.ImportTasks != nil {
		for _, task := range instance.ImportTasks.BuildSearchIndexTasks {
			buildSearchTasks[task.DimensionName] = buildSearchTask{
				isComplete:    task.State == dataset.StateCompleted.String(),
				dimensionName: task.DimensionName,
			}
		}
	}

	return buildSearchTasks
}

// getInstanceList gets a list of current import Instances, to seed our in-memory list
func (trackedInstances trackedInstanceList) getInstanceList(ctx context.Context, api *api.DatasetAPI) (bool, error) {
	instancesFromAPI, isFatal, err := api.GetInstances(ctx, url.Values{"state": []string{dataset.StateSubmitted.String()}})
	if err != nil {
		return isFatal, err
	}
	for _, instance := range instancesFromAPI.Items {

		hierarchyTasks := createHierarchyTaskMapFromInstance(instance)
		searchTasks := createSearchTaskMapFromInstance(instance)

		log.Event(ctx, "adding instance for tracking", log.INFO, log.Data{
			"instance_id":     instance.ID,
			"current_state":   instance.State,
			"hierarchy_tasks": hierarchyTasks,
			"search_tasks":    searchTasks,
		})

		if instance.ImportTasks != nil {
			trackedInstances[instance.ID] = trackedInstance{
				totalObservations:         instance.NumberOfObservations,
				observationsInsertedCount: instance.ImportTasks.ImportObservations.InsertedObservations,
				observationInsertComplete: instance.ImportTasks.ImportObservations.State == dataset.StateCompleted.String(),
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

	targetState := dataset.StateCompleted.String()
	for _, instanceRef := range importJobFromAPI.Links.Instances {
		if instanceRef.ID == completedInstanceID {
			continue
		}
		// XXX TODO code below largely untested, and possibly subject to race conditions
		instanceFromAPI, isFatal, err := datasetAPI.GetInstance(ctx, instanceRef.ID)
		if err != nil {
			return isFatal, err
		}
		if instanceFromAPI.State != dataset.StateCompleted.String() && instanceFromAPI.State != dataset.StateFailed.String() {
			return false, nil
		}
		if instanceFromAPI.State == dataset.StateFailed.String() {
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
	dataImportCompleteProducer *kafka.Producer,
	cfg *config.Config,
) {

	// inform main() when we have stopped processing events
	defer close(instanceLoopDoneChan)

	trackedInstances := make(trackedInstanceList)
	for i := 1; i <= cfg.InitialiseListAttempts; i++ {
		if _, err := trackedInstances.getInstanceList(ctx, datasetAPI); err != nil {
			if i == cfg.InitialiseListAttempts {
				log.Event(ctx, "failed to obtain initial instance list", log.ERROR, log.Error(err), log.Data{"attempt": i})
				return
			}
			log.Event(ctx, "could not obtain initial instance list - will retry", log.ERROR, log.Error(err), log.Data{"attempt": i})
			time.Sleep(cfg.InitialiseListInterval)
			continue
		}
		break
	}

	tickerChan := make(chan bool)
	go func() {
		for range time.Tick(cfg.CheckCompleteInterval) {
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
					log.Event(ctx, "could not update instance", log.ERROR, log.Error(err), log.Data{"instanceID": instanceID, "is_fatal": isFatal})
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
					countObservations, err := store.CountInsertedObservations(ctx, instanceID)
					if err != nil {
						log.Event(ctx, "Failed to check db for actual count(insertedObservations) now instance appears to be completed", log.ERROR, log.Error(err), logData)
					} else {
						logData["db_count"] = countObservations
						if countObservations != trackedInstances[instanceID].totalObservations {
							log.Event(ctx, "db_count of inserted observations != expected total - will continue to monitor", log.INFO, logData)
						} else {

							log.Event(ctx, "import observations complete, calling dataset api to set import observation task complete", log.INFO, logData)
							if isFatal, err := datasetAPI.SetImportObservationTaskComplete(ctx, instanceID); err != nil {
								logData["is_fatal"] = isFatal
								log.Event(ctx, "failed to set import observation task state=completed", log.ERROR, log.Error(err), logData)
								stopTracking = isFatal
							}

							err = produceDataImportCompleteEvents(ctx, trackedInstances[instanceID], instanceID, dataImportCompleteProducer)
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
					if isFatal, err := datasetAPI.UpdateInstanceState(ctx, instanceID, dataset.StateCompleted); err != nil {
						logData["is_fatal"] = isFatal
						log.Event(ctx, "failed to set import instance state=completed", log.ERROR, log.Error(err), logData)
						stopTracking = isFatal
					} else if isFatal, err := CheckImportJobCompletionState(ctx, importAPI, datasetAPI, trackedInstances[instanceID].jobID, instanceID); err != nil {
						logData["is_fatal"] = isFatal
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
				log.Event(ctx, "", log.ERROR, log.Error(errors.New("import instance exists")), log.Data{"instanceID": newInstanceMsg})
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
	log.Event(ctx, "Instance loop completed", log.INFO)
}

func produceDataImportCompleteEvents(
	ctx context.Context,
	instance trackedInstance,
	instanceID string,
	dataImportCompleteProducer *kafka.Producer) error {

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

		dataImportCompleteProducer.Channels().Output <- bytes
	}

	return nil
}

// logFatal is a utility method for a common failure pattern in main()
func logFatal(ctx context.Context, contextMessage string, err error, data log.Data) {
	log.Event(ctx, contextMessage, log.FATAL, log.Error(err), data)
	os.Exit(1)
}

func main() {
	log.Namespace = "dp-import-tracker"

	// create context for all work
	ctx, cancel := context.WithCancel(context.Background())

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)

	cfg, err := config.NewConfig()
	if err != nil {
		logFatal(ctx, "config failed", err, nil)
	}

	// sensitive fields are omitted from config.String().
	log.Event(ctx, "loaded config", log.INFO, log.Data{"config": cfg})

	kafkaOffset := kafka.OffsetNewest

	if cfg.KafkaOffsetOldest {
		kafkaOffset = kafka.OffsetOldest
	}

	cgLegacyConfig := &kafka.ConsumerGroupConfig{
		Offset:       &kafkaOffset,
		KafkaVersion: &cfg.KafkaLegacyVersion,
	}

	cgConfig := &kafka.ConsumerGroupConfig{
		Offset:       &kafkaOffset,
		KafkaVersion: &cfg.KafkaVersion,
	}
	if cfg.KafkaSecProtocol == "TLS" {
		cgConfig.SecurityConfig = kafka.GetSecurityConfig(
			cfg.KafkaSecCACerts,
			cfg.KafkaSecClientCert,
			cfg.KafkaSecClientKey,
			cfg.KafkaSecSkipVerify,
		)
	}

	// Create InstanceEvent kafka consumer - exit on channel validation error. Non-initialised consumers will not error at creation time.
	newInstanceEventConsumer, err := kafka.NewConsumerGroup(
		ctx,
		cfg.KafkaLegacyAddr,
		cfg.NewInstanceTopic,
		cfg.NewInstanceConsumerGroup,
		kafka.CreateConsumerGroupChannels(bufferSize),
		cgLegacyConfig,
	)

	if err != nil {
		logFatal(ctx, "could not obtain consumer", err, log.Data{"topic": cfg.NewInstanceTopic})
	}

	// Create ObservationsInsertedEvent kafka consumer - exit on channel validation error. Non-initialised consumers will not error at creation time.
	observationsInsertedEventConsumer, err := kafka.NewConsumerGroup(
		ctx,
		cfg.KafkaLegacyAddr,
		cfg.ObservationsInsertedTopic,
		cfg.ObservationsInsertedConsumerGroup,
		kafka.CreateConsumerGroupChannels(bufferSize),
		cgLegacyConfig,
	)

	if err != nil {
		logFatal(ctx, "could not obtain consumer", err, log.Data{"topic": cfg.ObservationsInsertedTopic})
	}

	// Create HierarchyBuilt kafka consumer - exit on channel validation error. Non-initialised consumers will not error at creation time.
	hierarchyBuiltConsumer, err := kafka.NewConsumerGroup(
		ctx,
		cfg.KafkaAddr,
		cfg.HierarchyBuiltTopic,
		cfg.HierarchyBuiltConsumerGroup,
		kafka.CreateConsumerGroupChannels(bufferSize),
		cgConfig,
	)

	if err != nil {
		logFatal(ctx, "could not obtain consumer", err, log.Data{"topic": cfg.HierarchyBuiltTopic})
	}

	// Create SearchBuilt kafka consumer - exit on channel validation error. Non-initialised consumers will not error at creation time.
	searchBuiltConsumer, err := kafka.NewConsumerGroup(
		ctx,
		cfg.KafkaAddr,
		cfg.SearchBuiltTopic,
		cfg.SearchBuiltConsumerGroup,
		kafka.CreateConsumerGroupChannels(bufferSize),
		cgConfig,
	)

	if err != nil {
		logFatal(ctx, "could not obtain consumer", err, log.Data{"topic": cfg.SearchBuiltTopic})
	}

	// Create DataImportComplete kafka producer - exit on channel validation error. Non-initialised producers will not error at creation time.

	pChannels := kafka.CreateProducerChannels()
	pConfig := &kafka.ProducerConfig{
		KafkaVersion: &cfg.KafkaVersion,
	}
	if cfg.KafkaSecProtocol == "TLS" {
		pConfig.SecurityConfig = kafka.GetSecurityConfig(
			cfg.KafkaSecCACerts,
			cfg.KafkaSecClientCert,
			cfg.KafkaSecClientKey,
			cfg.KafkaSecSkipVerify,
		)
	}

	dataImportCompleteProducer, err := kafka.NewProducer(
		ctx,
		cfg.KafkaAddr,
		cfg.DataImportCompleteTopic,
		pChannels,
		pConfig,
	)
	if err != nil {
		logFatal(ctx, "observation import complete kafka producer error", err, nil)
	}

	// Create GraphDB instance store
	graphDB, err := graph.NewInstanceStore(ctx)
	if err != nil {
		logFatal(ctx, "could not obtain database connection", err, nil)
	}
	graphErrorConsumer := graph.NewLoggingErrorConsumer(ctx, graphDB.Errors)

	// Create importAPI client
	importAPI := &api.ImportAPI{
		Client:           importapi.New(cfg.ImportAPIAddr),
		ServiceAuthToken: cfg.ServiceAuthToken,
	}

	// Create wrapped datasetAPI client
	datasetAPI := &api.DatasetAPI{
		Client:           dataset.NewAPIClient(cfg.DatasetAPIAddr),
		ServiceAuthToken: cfg.ServiceAuthToken,
		MaxWorkers:       cfg.DatasetAPIMaxWorkers,
		BatchSize:        cfg.DatasetAPIBatchSize,
	}

	// Create healthcheck object with versionInfo
	versionInfo, err := healthcheck.NewVersionInfo(BuildTime, GitCommit, Version)
	if err != nil {
		logFatal(ctx, "Failed to create versionInfo for healthcheck", err, log.Data{})
	}
	hc := healthcheck.New(versionInfo, cfg.HealthCheckCriticalTimeout, cfg.HealthCheckInterval)
	if err := api.RegisterCheckers(ctx, &hc,
		newInstanceEventConsumer,
		observationsInsertedEventConsumer,
		hierarchyBuiltConsumer,
		searchBuiltConsumer,
		dataImportCompleteProducer,
		importAPI.Client,
		datasetAPI.Client,
		graphDB); err != nil {
		os.Exit(1)
	}

	httpServerDoneChan := make(chan error)
	api.StartHealthCheck(ctx, &hc, cfg.BindAddr, httpServerDoneChan)

	updateInstanceWithObservationsInsertedChan := make(chan insertedObservationsEvent)
	createInstanceChan := make(chan events.InputFileAvailable)
	instanceLoopEndedChan := make(chan bool)

	go manageActiveInstanceEvents(
		ctx,
		createInstanceChan,
		updateInstanceWithObservationsInsertedChan,
		datasetAPI,
		importAPI,
		instanceLoopEndedChan,
		graphDB,
		dataImportCompleteProducer,
		cfg,
	)

	// Log non-fatal errors from kafka-consumers
	observationsInsertedEventConsumer.Channels().LogErrors(ctx, "ObservationInserted Consumer Error")
	newInstanceEventConsumer.Channels().LogErrors(ctx, "NewInstance Consumer Error")

	// loop over consumers messages and errors - in background, so we can attempt graceful shutdown
	// sends instance events to the (above) instance event handler
	mainLoopEndedChan := make(chan error)
	go func() {
		defer close(mainLoopEndedChan)
		var err error

		for looping := true; looping; {
			select {
			case <-ctx.Done():
				log.Event(ctx, "main loop aborting", log.INFO, log.Data{"ctx_err": ctx.Err()})
				looping = false
			case err = <-httpServerDoneChan:
				log.Event(ctx, "unexpected httpServer exit", log.ERROR, log.Error(err))
				looping = true
			case newInstanceMessage := <-newInstanceEventConsumer.Channels().Upstream:
				// This context will be obtained from the received kafka message in the future
				kafkaContext := context.Background()
				var newInstanceEvent events.InputFileAvailable
				if err = events.InputFileAvailableSchema.Unmarshal(newInstanceMessage.GetData(), &newInstanceEvent); err != nil {
					log.Event(kafkaContext, "TODO handle unmarshal error", log.ERROR, log.Error(err), log.Data{"topic": cfg.NewInstanceTopic})
				} else {
					createInstanceChan <- newInstanceEvent
				}
				newInstanceMessage.CommitAndRelease()
			case insertedMessage := <-observationsInsertedEventConsumer.Channels().Upstream:
				// This context will be obtained from the received kafka message in the future
				kafkaContext := context.Background()
				var insertedUpdate insertedObservationsEvent
				msg := insertedMessage.GetData()
				if err = events.ObservationsInsertedSchema.Unmarshal(msg, &insertedUpdate); err != nil {
					log.Event(kafkaContext, "unmarshal error", log.ERROR, log.Error(err), log.Data{"topic": cfg.ObservationsInsertedTopic, "msg": string(msg)})
				} else {
					insertedUpdate.DoneChan = make(chan insertResult)
					updateInstanceWithObservationsInsertedChan <- insertedUpdate
					if insertRes := <-insertedUpdate.DoneChan; insertRes == ErrRetry {
						// do not commit, update was non-fatal (will retry)
						log.Event(kafkaContext, "non-commit", log.ERROR, log.Error(errors.New("non-fatal error")), log.Data{"topic": cfg.ObservationsInsertedTopic, "msg": string(msg)})
						continue
					}
				}
				insertedMessage.CommitAndRelease()
			case hierarchyBuiltMessage := <-hierarchyBuiltConsumer.Channels().Upstream:
				// This context will be obtained from the received kafka message in the future
				kafkaContext := context.Background()
				var event events.HierarchyBuilt
				if err = events.HierarchyBuiltSchema.Unmarshal(hierarchyBuiltMessage.GetData(), &event); err != nil {

					// todo: call error reporter
					log.Event(kafkaContext, "unmarshal error", log.ERROR, log.Error(err), log.Data{"topic": cfg.HierarchyBuiltTopic})

				} else {

					logData := log.Data{"instance_id": event.InstanceID, "dimension_name": event.DimensionName}
					log.Event(kafkaContext, "processing hierarchy built message", log.INFO, logData)

					if isFatal, err := datasetAPI.UpdateInstanceWithHierarchyBuilt(
						kafkaContext,
						event.InstanceID,
						event.DimensionName); err != nil {

						if isFatal {
							// todo: call error reporter
							log.Event(kafkaContext, "failed to update instance with hierarchy built status", log.ERROR, log.Error(err), logData)
						}
					}
					log.Event(kafkaContext, "updated instance with hierarchy built. committing message", log.INFO, logData)
				}
				hierarchyBuiltMessage.CommitAndRelease()

			case searchBuiltMessage := <-searchBuiltConsumer.Channels().Upstream:
				// This context will be obtained from the received kafka message in the future
				kafkaContext := context.Background()
				var event events.SearchIndexBuilt
				if err = events.SearchIndexBuiltSchema.Unmarshal(searchBuiltMessage.GetData(), &event); err != nil {

					// todo: call error reporter
					log.Event(kafkaContext, "unmarshal error", log.ERROR, log.Error(err), log.Data{"topic": cfg.HierarchyBuiltTopic})

				} else {

					logData := log.Data{"instance_id": event.InstanceID, "dimension_name": event.DimensionName}
					log.Event(kafkaContext, "processing search index built message", log.INFO, logData)

					if isFatal, err := datasetAPI.UpdateInstanceWithSearchIndexBuilt(
						kafkaContext,
						event.InstanceID,
						event.DimensionName); err != nil {

						if isFatal {
							// todo: call error reporter
							log.Event(kafkaContext, "failed to update instance with hierarchy built status", log.ERROR, log.Error(err), logData)
						}
					}
					log.Event(kafkaContext, "updated instance with search index built. committing message", log.INFO, logData)
				}
				searchBuiltMessage.CommitAndRelease()
			}

		}
		log.Event(ctx, "main loop completed", log.INFO)
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
	log.Event(ctx, "Start shutdown", log.ERROR, log.Error(err), log.Data{"timeout": cfg.ShutdownTimeout})

	// Create context with timeout - Note: we can't reuse the main context, as we will cancel it before shutting down.
	shutdownContext, shutdownContextCancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)

	// tell main handler to stop
	// also tells any in-flight work (with this context) to stop
	cancel()

	// Shutdown step 1: healthcheck with and its HTTP server before starting to shutdown any other service.
	didTimeout := runInParallelWithTimeout(shutdownContext, []func(){
		func() {
			var err error
			if err = api.StopHealthCheck(shutdownContext, &hc); err != nil {
				log.Event(ctx, "bad healthcheck close", log.ERROR, log.Error(err))
			}
			<-httpServerDoneChan
		},
	})
	if didTimeout {
		log.Event(ctx, "Shutdown timed out at step 1 (healthcheck)", log.ERROR, log.Data{"context": shutdownContext.Err()})
		os.Exit(1)
	}

	// Shutdown step 2 all other dependencies can be shutted down in parallel
	didTimeout = runInParallelWithTimeout(shutdownContext, []func(){

		func() {
			// Shutdown ObservationsInsertedEventConsumer
			var err error
			if err = observationsInsertedEventConsumer.StopListeningToConsumer(shutdownContext); err != nil {
				log.Event(ctx, "bad listen stop", log.ERROR, log.Error(err), log.Data{"topic": cfg.ObservationsInsertedTopic})
			} else {
				log.Event(ctx, "listen stopped", log.INFO, log.Data{"topic": cfg.ObservationsInsertedTopic})
			}
			<-mainLoopEndedChan
			if err = observationsInsertedEventConsumer.Close(shutdownContext); err != nil {
				log.Event(ctx, "bad close", log.ERROR, log.Error(err), log.Data{"topic": cfg.ObservationsInsertedTopic})
			}
		},

		func() {
			// Shutdown NewInstanceEventConsumer and graphDB
			var err error
			if err = newInstanceEventConsumer.StopListeningToConsumer(shutdownContext); err != nil {
				log.Event(ctx, "bad listen stop", log.ERROR, log.Error(err), log.Data{"topic": cfg.NewInstanceTopic})
			} else {
				log.Event(ctx, "listen stopped", log.INFO, log.Data{"topic": cfg.NewInstanceTopic})
			}
			<-mainLoopEndedChan
			if err = newInstanceEventConsumer.Close(shutdownContext); err != nil {
				log.Event(ctx, "bad close", log.ERROR, log.Error(err), log.Data{"topic": cfg.NewInstanceTopic})
			}
			if err = graphDB.Close(shutdownContext); err != nil {
				log.Event(ctx, "bad db close", log.ERROR, log.Error(err), nil)
			}
			if err = graphErrorConsumer.Close(shutdownContext); err != nil {
				log.Event(ctx, "bad db error consumer close", log.ERROR, log.Error(err), nil)
			}
		},

		func() {
			// Shutdown hierarchyBuiltConsumer
			var err error
			if err = hierarchyBuiltConsumer.StopListeningToConsumer(shutdownContext); err != nil {
				log.Event(ctx, "bad listen stop", log.ERROR, log.Error(err), log.Data{"topic": cfg.HierarchyBuiltTopic})
			} else {
				log.Event(ctx, "listen stopped", log.INFO, log.Data{"topic": cfg.HierarchyBuiltTopic})
			}
			<-mainLoopEndedChan
			if err = hierarchyBuiltConsumer.Close(shutdownContext); err != nil {
				log.Event(ctx, "bad close", log.ERROR, log.Error(err), log.Data{"topic": cfg.HierarchyBuiltTopic})
			}
		},

		func() {
			// Shutdown SearchBuiltConsumer
			var err error
			if err = searchBuiltConsumer.StopListeningToConsumer(shutdownContext); err != nil {
				log.Event(ctx, "bad listen stop", log.ERROR, log.Error(err), log.Data{"topic": cfg.SearchBuiltTopic})
			} else {
				log.Event(ctx, "listen stopped", log.INFO, log.Data{"topic": cfg.SearchBuiltTopic})
			}
			<-mainLoopEndedChan
			if err = searchBuiltConsumer.Close(shutdownContext); err != nil {
				log.Event(ctx, "bad close", log.ERROR, log.Error(err), log.Data{"topic": cfg.SearchBuiltTopic})
			}
		},
	})
	if didTimeout {
		log.Event(ctx, "Shutdown timed out at step 2", log.ERROR, log.Data{"context": shutdownContext.Err()})
		os.Exit(1)
	}

	// Graceful shutdown was successful (all functions ended with no timeout)
	shutdownContextCancel()
	log.Event(ctx, "Done shutdown gracefully", log.INFO)
	os.Exit(0)
}

// runInParallelWithTimeout runs the provided functions in parallel, waiting for all of them to finish.
// It returns true only if the context timeout expires before all the functions finish their execution
func runInParallelWithTimeout(ctx context.Context, functions []func()) bool {
	wg := &sync.WaitGroup{}
	for _, parallelFunc := range functions {
		wg.Add(1)
		go func(f func()) {
			defer wg.Done()
			f()
		}(parallelFunc)
	}
	return waitWithTimeout(ctx, wg)
}

// waitWithTimeout blocks until all go-routines tracked by a WaitGroup are done, or until the timeout defined in a context expires.
// It returns true only if the context timeout expired
func waitWithTimeout(ctx context.Context, wg *sync.WaitGroup) bool {
	chWaiting := make(chan struct{})
	go func() {
		defer close(chWaiting)
		wg.Wait()
	}()
	select {
	case <-chWaiting:
		return false
	case <-ctx.Done():
		return true
	}
}
