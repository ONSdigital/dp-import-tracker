package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strconv"

	"github.com/ONSdigital/go-ns/log"
	"github.com/ONSdigital/go-ns/rchttp"
)

// DatasetAPI aggregates a client and url and other common data for accessing the API
type DatasetAPI struct {
	client    *rchttp.Client
	url       string
	authToken string
}

// NewDatasetAPI creates an DatasetAPI object
func NewDatasetAPI(client *rchttp.Client, url string, authToken string) *DatasetAPI {
	return &DatasetAPI{
		client:    client,
		url:       url,
		authToken: authToken,
	}
}

// InstanceResults wraps instances objects for pagination
type InstanceResults struct {
	Items []Instance `json:"items"`
}

// Instance comes in results from the Dataset API
type Instance struct {
	InstanceID                string        `json:"id"`
	Links                     InstanceLinks `json:"links,omitempty"`
	NumberOfObservations      int64         `json:"total_observations"`
	TotalInsertedObservations int64         `json:"total_inserted_observations,omitempty"`
	State                     string        `json:"state"`
}

// InstanceLinks holds all links for an instance
type InstanceLinks struct {
	Job JobLinks `json:"job"`
}

// JobLinks holds the id and a link to the resource
type JobLinks struct {
	ID   string `json:"id"`
	HRef string `json:"href"`
}

// GetInstance asks the Dataset API for the details for instanceID
func (api *DatasetAPI) GetInstance(ctx context.Context, instanceID string) (instance Instance, isFatal bool, err error) {
	path := api.url + "/instances/" + instanceID
	logData := log.Data{"path": path, "instanceID": instanceID}
	jsonBody, httpCode, err := api.get(ctx, path, nil)
	logData["jsonBody"] = jsonBody
	if isFatal, err = errorChecker("GetInstance", err, httpCode, &logData); err != nil {
		return
	}

	if err = json.Unmarshal(jsonBody, &instance); err != nil {
		log.ErrorC("GetInstance unmarshall", err, logData)
		isFatal = true
	}
	return
}

// GetInstances asks the Dataset API for all instances filtered by vars
func (api *DatasetAPI) GetInstances(ctx context.Context, vars url.Values) (instances []Instance, isFatal bool, err error) {
	path := api.url + "/instances"
	logData := log.Data{"path": path}
	jsonBody, httpCode, err := api.get(ctx, path, vars)
	logData["jsonBody"] = jsonBody
	if isFatal, err = errorChecker("GetInstances", err, httpCode, &logData); err != nil {
		return
	}

	var instanceResults InstanceResults
	if err = json.Unmarshal(jsonBody, &instanceResults); err != nil {
		log.ErrorC("GetInstances Unmarshal", err, logData)
		return instances, true, err
	}
	return instanceResults.Items, isFatal, nil
}

// UpdateInstanceWithNewInserts tells the Dataset API of a number of observationsInserted for instanceID
func (api *DatasetAPI) UpdateInstanceWithNewInserts(ctx context.Context, instanceID string, observationsInserted int32) (isFatal bool, err error) {
	path := api.url + "/instances/" + instanceID + "/inserted_observations/" + strconv.FormatInt(int64(observationsInserted), 10)
	logData := log.Data{"url": path}
	jsonBody, httpCode, err := api.put(ctx, path, nil)
	logData["jsonBytes"] = jsonBody
	return errorChecker("UpdateInstanceWithNewInserts", err, httpCode, &logData)
}

// UpdateInstanceState tells the Dataset API that the state has changed of an Dataset instance
func (api *DatasetAPI) UpdateInstanceState(ctx context.Context, instanceID string, newState string) (isFatal bool, err error) {
	path := api.url + "/instances/" + instanceID
	logData := log.Data{"url": path}
	jsonUpload := []byte(`{"state":"` + newState + `"}`)
	logData["jsonUpload"] = jsonUpload
	jsonResult, httpCode, err := api.put(ctx, path, jsonUpload)
	logData["jsonResult"] = jsonResult
	return errorChecker("UpdateInstanceState", err, httpCode, &logData)
}

func errorChecker(tag string, err error, httpCode int, logData *log.Data) (isFatal bool, returnedError error) {
	(*logData)["httpCode"] = httpCode
	if err == nil && httpCode != http.StatusOK {
		// this error logged at end of func
		returnedError = errors.New("Bad http response")
		if httpCode < http.StatusInternalServerError {
			isFatal = true
		}
	} else if err != nil {
		// this error logged at end of func
		returnedError = err
		isFatal = true
	}
	if returnedError != nil {
		(*logData)["is_fatal"] = isFatal
		log.ErrorC(tag, returnedError, *logData)
	}
	return
}

func (api *DatasetAPI) get(ctx context.Context, path string, vars url.Values) ([]byte, int, error) {
	return callAPI(ctx, api.client, "GET", path, api.authToken, vars)
}

func (api *DatasetAPI) put(ctx context.Context, path string, payload []byte) ([]byte, int, error) {
	return callAPI(ctx, api.client, "PUT", path, api.authToken, payload)
}
