package api

import (
	"context"
	"net/url"

	dataset "github.com/ONSdigital/dp-api-clients-go/dataset"
)

//go:generate moq -out ./mock/dataset.go -pkg mock . DatasetClient

// DatasetClient is an interface to represent methods called to action upon Dataset REST interface
type DatasetClient interface {
	GetInstance(ctx context.Context, userAuthToken, serviceAuthToken, collectionID, instanceID string) (m dataset.Instance, err error)
	GetInstances(ctx context.Context, userAuthToken, serviceAuthToken, collectionID string, vars url.Values) (m dataset.Instances, err error)
	PutInstanceImportTasks(ctx context.Context, serviceAuthToken, instanceID string, data dataset.InstanceImportTasks) error
	UpdateInstanceWithNewInserts(ctx context.Context, serviceAuthToken, instanceID string, observationsInserted int32) error
	PutInstanceState(ctx context.Context, serviceAuthToken, instanceID string, state dataset.State) error
}
