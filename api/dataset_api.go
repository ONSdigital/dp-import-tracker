package api

import (
	"context"
	"net/url"

	dataset "github.com/ONSdigital/dp-api-clients-go/dataset"
	"github.com/ONSdigital/log.go/log"
)

// DatasetAPI extends the dataset api Client with json - bson mapping, specific calls, and error management
type DatasetAPI struct {
	ServiceAuthToken string
	Client           DatasetClient
	MaxWorkers       int
	BatchSize        int
}

// GetInstance asks the Dataset API for the details for instanceID
func (api *DatasetAPI) GetInstance(ctx context.Context, instanceID string) (instance dataset.Instance, isFatal bool, err error) {
	instance, err = api.Client.GetInstance(ctx, "", api.ServiceAuthToken, "", instanceID)
	isFatal = errorChecker(ctx, "GetInstance", err, &log.Data{"instanceID": instanceID})
	return
}

// GetInstances asks the Dataset API for all instances filtered by vars
func (api *DatasetAPI) GetInstances(ctx context.Context, vars url.Values) (instances dataset.Instances, isFatal bool, err error) {
	instances, err = api.Client.GetInstancesInBatches(ctx, "", api.ServiceAuthToken, "", vars, api.BatchSize, api.MaxWorkers)
	isFatal = errorChecker(ctx, "GetInstance", err, &log.Data{})
	return
}

// SetImportObservationTaskComplete marks the import observation task state as completed for an instance
func (api *DatasetAPI) SetImportObservationTaskComplete(ctx context.Context, instanceID string) (isFatal bool, err error) {
	err = api.Client.PutInstanceImportTasks(ctx, api.ServiceAuthToken, instanceID,
		dataset.InstanceImportTasks{
			ImportObservations: &dataset.ImportObservationsTask{
				State: dataset.StateCompleted.String(),
			},
		},
	)
	isFatal = errorChecker(ctx, "SetImportObservationTaskComplete", err, &log.Data{})
	return
}

// UpdateInstanceWithNewInserts increments the observation inserted count for an instance
func (api *DatasetAPI) UpdateInstanceWithNewInserts(ctx context.Context, instanceID string, observationsInserted int32) (isFatal bool, err error) {
	err = api.Client.UpdateInstanceWithNewInserts(ctx, api.ServiceAuthToken, instanceID, observationsInserted)
	isFatal = errorChecker(ctx, "UpdateInstanceWithNewInserts", err, &log.Data{})
	return
}

// UpdateInstanceWithHierarchyBuilt marks a hierarchy build task state as completed for an instance.
func (api *DatasetAPI) UpdateInstanceWithHierarchyBuilt(ctx context.Context, instanceID, dimensionID string) (isFatal bool, err error) {
	err = api.Client.PutInstanceImportTasks(ctx, api.ServiceAuthToken, instanceID,
		dataset.InstanceImportTasks{
			BuildHierarchyTasks: []*dataset.BuildHierarchyTask{
				&dataset.BuildHierarchyTask{
					DimensionName: dimensionID,
					State:         dataset.StateCompleted.String(),
				},
			},
		},
	)
	isFatal = errorChecker(ctx, "UpdateInstanceWithHierarchyBuilt", err, &log.Data{})
	return
}

// UpdateInstanceWithSearchIndexBuilt marks a search index build task state as completed for an instance.
func (api *DatasetAPI) UpdateInstanceWithSearchIndexBuilt(ctx context.Context, instanceID, dimensionID string) (isFatal bool, err error) {
	err = api.Client.PutInstanceImportTasks(ctx, api.ServiceAuthToken, instanceID,
		dataset.InstanceImportTasks{
			BuildSearchIndexTasks: []*dataset.BuildSearchIndexTask{
				&dataset.BuildSearchIndexTask{
					DimensionName: dimensionID,
					State:         dataset.StateCompleted.String(),
				},
			},
		},
	)
	isFatal = errorChecker(ctx, "UpdateInstanceWithHierarchyBuilt", err, &log.Data{})
	return
}

// UpdateInstanceState tells the Dataset API that the state has changed of a Dataset instance
func (api *DatasetAPI) UpdateInstanceState(ctx context.Context, instanceID string, newState dataset.State) (isFatal bool, err error) {
	err = api.Client.PutInstanceState(ctx, api.ServiceAuthToken, instanceID, newState)
	isFatal = errorChecker(ctx, "UpdateInstanceWithHierarchyBuilt", err, &log.Data{})
	return
}
