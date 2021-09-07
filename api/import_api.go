package api

import (
	"context"

	importapi "github.com/ONSdigital/dp-api-clients-go/v2/importapi"
	"github.com/ONSdigital/log.go/v2/log"
)

// ImportAPI extends the import api Client with error management and service token
type ImportAPI struct {
	ServiceAuthToken string
	Client           ImportAPIClient
}

// GetImportJob wraps GetImportJob from importAPI, and determines if the error is fatal.
func (i *ImportAPI) GetImportJob(ctx context.Context, importJobID string) (importJob importapi.ImportJob, isFatal bool, err error) {
	importJob, err = i.Client.GetImportJob(ctx, importJobID, i.ServiceAuthToken)
	isFatal = errorChecker(ctx, "GetImportJob", err, &log.Data{})
	return
}

// UpdateImportJobState wraps UpdateImportJobState from importAPI.
func (i *ImportAPI) UpdateImportJobState(ctx context.Context, jobID, newState string) error {
	return i.Client.UpdateImportJobState(ctx, jobID, i.ServiceAuthToken, newState)
}
