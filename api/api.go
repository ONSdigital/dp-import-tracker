package api

import (
	"context"
	"net/http"
	"net/url"

	dataset "github.com/ONSdigital/dp-api-clients-go/dataset"
	importapi "github.com/ONSdigital/dp-api-clients-go/importapi"
	"github.com/ONSdigital/dp-healthcheck/healthcheck"
	"github.com/ONSdigital/log.go/log"
)

//go:generate moq -out ./mock/dataset.go -pkg mock . DatasetClient
//go:generate moq -out ./mock/import.go -pkg mock . ImportAPIClient

// DatasetClient is an interface to represent methods called to action upon Dataset REST interface
type DatasetClient interface {
	GetInstance(ctx context.Context, userAuthToken, serviceAuthToken, collectionID, instanceID string) (m dataset.Instance, err error)
	GetInstances(ctx context.Context, userAuthToken, serviceAuthToken, collectionID string, vars url.Values) (m dataset.Instances, err error)
	PutInstanceImportTasks(ctx context.Context, serviceAuthToken, instanceID string, data dataset.InstanceImportTasks) error
	UpdateInstanceWithNewInserts(ctx context.Context, serviceAuthToken, instanceID string, observationsInserted int32) error
	PutInstanceState(ctx context.Context, serviceAuthToken, instanceID string, state dataset.State) error
	Checker(ctx context.Context, check *healthcheck.CheckState) error
}

// ImportAPIClient is an interface to represent methods called to action upon Import API REST interface
type ImportAPIClient interface {
	GetImportJob(ctx context.Context, importJobID, serviceToken string) (importJob importapi.ImportJob, err error)
	UpdateImportJobState(ctx context.Context, jobID, serviceToken string, newState string) error
	Checker(ctx context.Context, check *healthcheck.CheckState) error
}

// errorChecker determines if an error is fatal. Only errors corresponding to http responses on the range 500+ will be considered non-fatal.
func errorChecker(ctx context.Context, tag string, err error, logData *log.Data) (isFatal bool) {
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
	case *importapi.ErrInvalidAPIResponse:
		httpCode := err.(*importapi.ErrInvalidAPIResponse).Code()
		(*logData)["httpCode"] = httpCode
		if httpCode < http.StatusInternalServerError {
			isFatal = true
		}
	default:
		isFatal = true
	}
	(*logData)["is_fatal"] = isFatal
	log.Event(ctx, tag, log.ERROR, log.Error(err), *logData)
	return
}
