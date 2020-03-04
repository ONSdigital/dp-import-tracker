package api

import (
	"context"
	"net/http"
	"net/url"

	dataset "github.com/ONSdigital/dp-api-clients-go/dataset"
	"github.com/ONSdigital/log.go/log"
)

// DatasetAPI extends the dataset api Client with json - bson mapping, specific calls, and error management
type DatasetAPI struct {
	ServiceAuthToken string
	Client           DatasetClient
}

// InstanceImportTasks represents all of the tasks required to complete an import job.
type InstanceImportTasks struct {
	ImportObservations    *ImportObservationsTask `bson:"import_observations,omitempty"  json:"import_observations"`
	BuildHierarchyTasks   []*BuildHierarchyTask   `bson:"build_hierarchies,omitempty"    json:"build_hierarchies"`
	BuildSearchIndexTasks []*BuildSearchIndexTask `bson:"build_search_indexes,omitempty" json:"build_search_indexes"`
}

// ImportObservationsTask represents the task of importing instance observation data into the database.
type ImportObservationsTask struct {
	State                string `bson:"state,omitempty"             json:"state,omitempty"`
	InsertedObservations int64  `bson:"total_inserted_observations" json:"total_inserted_observations"`
}

// BuildHierarchyTask represents a task of importing a single hierarchy.
type BuildHierarchyTask struct {
	State         string `bson:"state,omitempty"          json:"state,omitempty"`
	DimensionName string `bson:"dimension_name,omitempty" json:"dimension_name,omitempty"`
	CodeListID    string `bson:"code_list_id,omitempty"   json:"code_list_id,omitempty"`
}

// BuildSearchIndexTask represents a task of importing a single search index into search.
type BuildSearchIndexTask struct {
	State         string `bson:"state,omitempty"          json:"state,omitempty"`
	DimensionName string `bson:"dimension_name,omitempty" json:"dimension_name,omitempty"`
}

// TODO map json models to bson models

// GetInstance asks the Dataset API for the details for instanceID
func (api *DatasetAPI) GetInstance(ctx context.Context, instanceID string) (instance dataset.Instance, isFatal bool, err error) {
	instance, err = api.Client.GetInstance(ctx, "", api.ServiceAuthToken, "", instanceID)
	isFatal = errorChecker("GetInstance", err, &log.Data{"instanceID": instanceID})
	return
}

// GetInstances asks the Dataset API for all instances filtered by vars
func (api *DatasetAPI) GetInstances(ctx context.Context, vars url.Values) (instances dataset.Instances, isFatal bool, err error) {
	instances, err = api.Client.GetInstances(ctx, "", api.ServiceAuthToken, "", vars)
	isFatal = errorChecker("GetInstance", err, &log.Data{})
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
	isFatal = errorChecker("SetImportObservationTaskComplete", err, &log.Data{})
	return
}

// UpdateInstanceWithNewInserts increments the observation inserted count for an instance
func (api *DatasetAPI) UpdateInstanceWithNewInserts(ctx context.Context, instanceID string, observationsInserted int32) (isFatal bool, err error) {
	err = api.Client.UpdateInstanceWithNewInserts(ctx, api.ServiceAuthToken, instanceID, observationsInserted)
	isFatal = errorChecker("UpdateInstanceWithNewInserts", err, &log.Data{})
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
	isFatal = errorChecker("UpdateInstanceWithHierarchyBuilt", err, &log.Data{})
	return
}

// UpdateInstanceWithSearchIndexBuilt marks a search index build task state as completed for an instance.
func (api *DatasetAPI) UpdateInstanceWithSearchIndexBuilt(ctx context.Context, instanceID, dimensionID string) (isFatal bool, err error) {
	err = api.Client.PutInstanceImportTasks(ctx, api.ServiceAuthToken, instanceID,
		dataset.InstanceImportTasks{
			BuildSearchIndexTasks: []*dataset.BuildSearchIndexTask{
				&dataset.BuildSearchIndexTask{
					DimensionName: dimensionID,
					State:         dataset.StateSubmitted.String(),
				},
			},
		},
	)
	isFatal = errorChecker("UpdateInstanceWithHierarchyBuilt", err, &log.Data{})
	return
}

// UpdateInstanceState tells the Dataset API that the state has changed of a Dataset instance
func (api *DatasetAPI) UpdateInstanceState(ctx context.Context, instanceID string, newState dataset.State) (isFatal bool, err error) {
	err = api.Client.PutInstanceState(ctx, api.ServiceAuthToken, instanceID, newState)
	isFatal = errorChecker("UpdateInstanceWithHierarchyBuilt", err, &log.Data{})
	return
}

func errorChecker(tag string, err error, logData *log.Data) (isFatal bool) {
	if err == nil {
		return false
	}
	switch err.(type) {
	case *dataset.ErrInvalidDatasetAPIResponse:
		httpCode := err.(*dataset.ErrInvalidDatasetAPIResponse).Code()
		(*logData)["httpCode"] = httpCode
		if httpCode < http.StatusInternalServerError {
			isFatal = true
		}
	default:
		isFatal = true
	}
	(*logData)["is_fatal"] = isFatal
	log.Event(context.Background(), tag, log.ERROR, log.Error(err), *logData)
	return
}
