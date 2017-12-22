package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"

	"github.com/ONSdigital/go-ns/log"
	"github.com/ONSdigital/go-ns/rchttp"
)

// ImportAPI aggregates a client and url and other common data for accessing the API
type ImportAPI struct {
	client    *rchttp.Client
	url       string
	authToken string
}

// ImportJob comes from the Import API and links an import job to its (other) instances
type ImportJob struct {
	JobID string  `json:"id"`
	Links LinkMap `json:"links,ignoreempty"`
}

// LinkMap is an array of instance links associated with am import job
type LinkMap struct {
	Instances []InstanceLink `json:"instances"`
}

// InstanceLink identifies an (instance or import-job) by id and url (from Import API)
type InstanceLink struct {
	ID   string `json:"id"`
	Link string `json:"href"`
}

// NewImportAPI creates an ImportAPI object
func NewImportAPI(client *rchttp.Client, url, authToken string) *ImportAPI {
	return &ImportAPI{
		client:    client,
		url:       url,
		authToken: authToken,
	}
}

// GetImportJob asks the Import API for the details for an Import job
func (api *ImportAPI) GetImportJob(ctx context.Context, importJobID string) (importJob ImportJob, isFatal bool, err error) {
	path := api.url + "/jobs/" + importJobID
	logData := log.Data{"path": path, "job_id": importJobID}
	jsonBody, httpCode, err := api.get(ctx, path, 0, nil)
	if httpCode == http.StatusNotFound {
		err = nil
		return
	}
	logData["httpCode"] = httpCode
	if err == nil && httpCode != http.StatusOK {
		if httpCode < http.StatusInternalServerError {
			isFatal = true
		}
		err = errors.New("Bad response while getting import job")
	} else {
		isFatal = true
	}
	if err != nil {
		log.ErrorC("GetImportJob", err, logData)
		return
	}
	logData["jsonBody"] = string(jsonBody)

	if err = json.Unmarshal(jsonBody, &importJob); err != nil {
		log.ErrorC("GetImportJob unmarshall", err, logData)
		isFatal = true
	}

	return
}

// UpdateImportJobState tells the Import API that the state has changed of an Import job
func (api *ImportAPI) UpdateImportJobState(ctx context.Context, jobID string, newState string) error {
	path := api.url + "/jobs/" + jobID
	logData := log.Data{"url": path}
	jsonUpload := []byte(`{"state":"` + newState + `"}`)
	logData["jsonUpload"] = jsonUpload
	jsonResult, httpCode, err := api.put(ctx, path, 0, jsonUpload)
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

func (api *ImportAPI) get(ctx context.Context, path string, attempts int, vars url.Values) ([]byte, int, error) {
	return callAPI(ctx, api.client, "GET", path, api.authToken, vars)
}

func (api *ImportAPI) put(ctx context.Context, path string, attempts int, payload []byte) ([]byte, int, error) {
	return callAPI(ctx, api.client, "PUT", path, api.authToken, payload)
}
