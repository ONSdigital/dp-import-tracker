package api

import (
	"encoding/json"
	"errors"
	"github.com/ONSdigital/go-ns/log"
	"net/http"
	"net/url"
	"strconv"
)

// DatasetAPI aggregates a client and URL and other common data for accessing the API
type DatasetAPI struct {
	Client    *http.Client
	URL       string
	AuthToken string
}

// NewDatasetAPI creates an DatasetAPI object
func NewDatasetAPI(client *http.Client, url string, authToken string) *DatasetAPI {
	return &DatasetAPI{
		Client:    client,
		URL:       url,
		AuthToken: authToken,
	}
}

// Instance comes in results from the Dataset API
type Instance struct {
	InstanceID                string `json:"instance_id"`
	NumberOfObservations      int64  `json:"total_observations"`
	TotalInsertedObservations int64  `json:"total_inserted_observations,omitempty"`
	Job                       IDLink `json:"job"`
	State                     string `json:"state"`
}

// GetInstance asks the Dataset API for the details for instanceID
func (api *DatasetAPI) GetInstance(instanceID string) (Instance, error) {
	path := api.URL + "/instances/" + instanceID
	logData := log.Data{"path": path, "instanceID": instanceID}
	jsonBody, httpCode, err := api.get(path, 0, nil)
	logData["httpCode"] = httpCode
	if httpCode == http.StatusNotFound {
		return Instance{}, nil
	}
	if err == nil && httpCode != http.StatusOK {
		err = errors.New("Bad response while getting dataset instance")
	}
	if err != nil {
		log.ErrorC("GetInstance get", err, logData)
		return Instance{}, err
	}
	logData["jsonBody"] = jsonBody

	var instance Instance
	if err := json.Unmarshal(jsonBody, &instance); err != nil {
		log.ErrorC("GetInstance unmarshall", err, logData)
		return Instance{}, err
	}
	return instance, nil
}

// GetInstances asks the Dataset API for all instances filtered by vars
func (api *DatasetAPI) GetInstances(vars url.Values) ([]Instance, error) {
	path := api.URL + "/instances"
	logData := log.Data{"path": path}
	jsonBody, httpCode, err := api.get(path, 0, vars)
	logData["httpCode"] = httpCode
	if err == nil && httpCode != http.StatusOK {
		err = errors.New("Bad response while getting dataset instances")
	}
	if err != nil {
		log.ErrorC("GetInstances get", err, logData)
		return nil, err
	}
	logData["jsonBody"] = jsonBody

	var instances []Instance
	if err := json.Unmarshal(jsonBody, &instances); err != nil {
		log.ErrorC("GetInstances Unmarshal", err, logData)
		return nil, err
	}
	return instances, nil
}

// UpdateInstanceWithNewInserts tells the Dataset API of a number of observationsInserted for instanceID
func (api *DatasetAPI) UpdateInstanceWithNewInserts(instanceID string, observationsInserted int32) error {
	path := api.URL + "/instances/" + instanceID + "/inserted_observations/" + strconv.FormatInt(int64(observationsInserted), 10)
	logData := log.Data{"URL": path}
	jsonBody, httpCode, err := api.put(path, 0, nil)
	logData["httpCode"] = httpCode
	logData["jsonBytes"] = jsonBody
	if err == nil && httpCode != http.StatusOK {
		err = errors.New("Bad response while updating inserts for job")
	}
	if err != nil {
		log.ErrorC("UpdateInstanceWithNewInserts err", err, logData)
		return err
	}
	return nil
}

// UpdateInstanceState tells the Dataset API that the state has changed of an Dataset instance
func (api *DatasetAPI) UpdateInstanceState(instanceID string, newState string) error {
	path := api.URL + "/instances/" + instanceID
	logData := log.Data{"URL": path}
	jsonUpload := []byte(`{"instance_id":"` + instanceID + `","state":"` + newState + `"}`)
	logData["jsonUpload"] = jsonUpload
	jsonResult, httpCode, err := api.put(path, 0, jsonUpload)
	logData["httpCode"] = httpCode
	logData["jsonResult"] = jsonResult
	if err == nil && httpCode != http.StatusOK {
		err = errors.New("Bad response while updating instance state")
	}
	if err != nil {
		log.ErrorC("UpdateInstanceState", err, logData)
		return err
	}
	return nil
}

func (api *DatasetAPI) get(path string, attempts int, vars url.Values) ([]byte, int, error) {
	return callAPI(api.Client, "GET", path, api.AuthToken, maxRetries, attempts, vars)
}

func (api *DatasetAPI) put(path string, attempts int, payload []byte) ([]byte, int, error) {
	return callAPI(api.Client, "PUT", path, api.AuthToken, maxRetries, attempts, payload)
}