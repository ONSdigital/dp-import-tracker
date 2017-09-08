package api

import (
	"encoding/json"
	"errors"
	"github.com/ONSdigital/go-ns/log"
	"net/http"
	"net/url"
)

// ImportAPI aggregates a client and URL and other common data for accessing the API
type ImportAPI struct {
	Client    *http.Client
	URL       string
	AuthToken string
}

// ImportJob comes from the Import API and links an import job to its (other) instances
type ImportJob struct {
	JobID     string         `json:"job_id"`
	Instances []InstanceLink `json:"instances"`
}

// InstanceLink identifies an (instance or import-job) by id and URL (from Import API)
type InstanceLink struct {
	ID   string `json:"id"`
	Link string `json:"link"`
}

// NewImportAPI creates an ImportAPI object
func NewImportAPI(client *http.Client, url, authToken string) *ImportAPI {
	return &ImportAPI{
		Client:    client,
		URL:       url,
		AuthToken: authToken,
	}
}

// GetImportJob asks the Import API for the details for an Import job
func (api *ImportAPI) GetImportJob(importJobID string) (ImportJob, error) {
	path := api.URL + "/jobs/" + importJobID
	logData := log.Data{"path": path, "importJobID": importJobID}
	jsonBody, httpCode, err := api.get(path, 0, nil)
	if httpCode == http.StatusNotFound {
		return ImportJob{}, nil
	}
	logData["httpCode"] = httpCode
	if err == nil && httpCode != http.StatusOK {
		err = errors.New("Bad response while getting import job")
	}
	if err != nil {
		log.ErrorC("GetImportJob", err, logData)
		return ImportJob{}, err
	}
	logData["jsonBody"] = string(jsonBody)

	var importJob ImportJob
	if err := json.Unmarshal(jsonBody, &importJob); err != nil {
		log.ErrorC("GetImportJob unmarshall", err, logData)
		return ImportJob{}, err
	}

	return importJob, nil
}

// UpdateImportJobState tells the Import API that the state has changed of an Import job
func (api *ImportAPI) UpdateImportJobState(jobID string, newState string) error {
	path := api.URL + "/jobs/" + jobID
	logData := log.Data{"URL": path}
	jsonUpload := []byte(`{"job_id":"` + jobID + `","state":"` + newState + `"}`)
	logData["jsonUpload"] = jsonUpload
	jsonResult, httpCode, err := api.put(path, 0, jsonUpload)
	logData["httpCode"] = httpCode
	logData["jsonResult"] = jsonResult
	if err == nil && httpCode != http.StatusOK {
		err = errors.New("Bad HTTP response")
	}
	if err != nil {
		log.ErrorC("UpdateImportJobState", err, logData)
		return err
	}
	return nil
}

func (api *ImportAPI) get(path string, attempts int, vars url.Values) ([]byte, int, error) {
	return callAPI(api.Client, "GET", path, api.AuthToken, maxRetries, attempts, vars)
}

func (api *ImportAPI) put(path string, attempts int, payload []byte) ([]byte, int, error) {
	return callAPI(api.Client, "PUT", path, api.AuthToken, maxRetries, attempts, payload)
}
