package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strconv"

	"github.com/ONSdigital/go-ns/log"
	"github.com/ONSdigital/go-ns/rhttp"
)

// DatasetAPI aggregates a client and url and other common data for accessing the API
type DatasetAPI struct {
	client    *rhttp.Client
	url       string
	authToken string
}

// NewDatasetAPI creates an DatasetAPI object
func NewDatasetAPI(client *rhttp.Client, url string, authToken string) *DatasetAPI {
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

// InstanceLink holds the id and a link to the resource
type JobLinks struct {
	ID   string `json:"id"`
	HRef string `json:"href"`
}

// GetInstance asks the Dataset API for the details for instanceID
func (api *DatasetAPI) GetInstance(instanceID string) (Instance, error) {
	path := api.url + "/instances/" + instanceID
	logData := log.Data{"path": path, "instanceID": instanceID}
	jsonBody, httpCode, err := api.get(path, nil)
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
	path := api.url + "/instances"
	logData := log.Data{"path": path}
	jsonBody, httpCode, err := api.get(path, vars)
	logData["httpCode"] = httpCode
	if err == nil && httpCode != http.StatusOK {
		err = errors.New("Bad response while getting dataset instances")
	}
	if err != nil {
		log.ErrorC("GetInstances get", err, logData)
		return nil, err
	}
	logData["jsonBody"] = jsonBody

	var instanceResults InstanceResults
	if err := json.Unmarshal(jsonBody, &instanceResults); err != nil {
		log.ErrorC("GetInstances Unmarshal", err, logData)
		return nil, err
	}
	return instanceResults.Items, nil
}

// UpdateInstanceWithNewInserts tells the Dataset API of a number of observationsInserted for instanceID
func (api *DatasetAPI) UpdateInstanceWithNewInserts(instanceID string, observationsInserted int32) error {
	path := api.url + "/instances/" + instanceID + "/inserted_observations/" + strconv.FormatInt(int64(observationsInserted), 10)
	logData := log.Data{"url": path}
	jsonBody, httpCode, err := api.put(path, nil)
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
	path := api.url + "/instances/" + instanceID
	logData := log.Data{"url": path}
	jsonUpload := []byte(`{"instance_id":"` + instanceID + `","state":"` + newState + `"}`)
	logData["jsonUpload"] = jsonUpload
	jsonResult, httpCode, err := api.put(path, jsonUpload)
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

func (api *DatasetAPI) get(path string, vars url.Values) ([]byte, int, error) {
	return callAPI(api.client, "GET", path, api.authToken, vars)
}

func (api *DatasetAPI) put(path string, payload []byte) ([]byte, int, error) {
	return callAPI(api.client, "PUT", path, api.authToken, payload)
}
