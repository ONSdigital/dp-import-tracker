package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/ONSdigital/go-ns/log"
)

// AuthToken is set by main()
var (
	AuthToken  string
	maxRetries = 5
)

// ImportAPI aggreagates a client and URL and other common data for accessing the API
type ImportAPI struct {
	Client     *http.Client
	URL        string
	MaxRetries int
}

// Instance comes in results from the Import API
type Instance struct {
	InstanceID                string `json:"instance_id"`
	NumberOfObservations      int64  `json:"total_observations"`
	TotalInsertedObservations int64  `json:"total_inserted_observations,omitempty"`
	Job                       IDLink `json:"job"`
	State                     string `json:"state"`
}

// ImportJob comes from the Import API and links an import job to its (other) instances
type ImportJob struct {
	JobID     string   `json:"job_id"`
	Instances []IDLink `json:"instances"`
}

// IDLink identifies an (instance or import-job) by id and URL (from Import API)
type IDLink struct {
	ID   string `json:"id"`
	Link string `json:"link"`
}

// New creates an ImportAPI object
func New(client *http.Client, importAPIURL string) (*ImportAPI, error) {
	return &ImportAPI{
		Client:     client,
		URL:        importAPIURL,
		MaxRetries: maxRetries,
	}, nil
}

// GetInstance asks the Import API for the details for instanceID
func (api *ImportAPI) GetInstance(instanceID string) (Instance, error) {
	path := api.URL + "/instances/" + instanceID
	logData := log.Data{"path": path, "instanceID": instanceID}
	jsonBody, httpCode, err := api.get(path, 0, nil)
	logData["httpCode"] = httpCode
	if httpCode == http.StatusNotFound {
		return Instance{}, nil
	}
	if err == nil && httpCode != http.StatusOK {
		err = errors.New("Bad response while getting import instance")
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

// GetInstances asks the Import API for all instances filtered by vars
func (api *ImportAPI) GetInstances(vars url.Values) ([]Instance, error) {
	path := api.URL + "/instances"
	logData := log.Data{"path": path}
	jsonBody, httpCode, err := api.get(path, 0, vars)
	logData["httpCode"] = httpCode
	if err == nil && httpCode != http.StatusOK {
		err = errors.New("Bad response while getting import instances")
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

// UpdateInstanceWithNewInserts tells the Import API of a number of observationsInserted for instanceID
func (api *ImportAPI) UpdateInstanceWithNewInserts(instanceID string, observationsInserted int32) error {
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

// UpdateInstanceState tells the Import API that the state has changed of an Import instance
func (api *ImportAPI) UpdateInstanceState(instanceID string, newState string) error {
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
	return api.callImportAPI("GET", path, attempts, vars)
}

func (api *ImportAPI) put(path string, attempts int, payload []byte) ([]byte, int, error) {
	return api.callImportAPI("PUT", path, attempts, payload)
}

// callImportAPI contacts the Import API returns the json body (action = PUT, GET, POST, ...)
func (api *ImportAPI) callImportAPI(method, path string, attempts int, payload interface{}) ([]byte, int, error) {
	logData := log.Data{"URL": path, "method": method, "attempts": attempts}

	if attempts == 0 {
		URL, err := url.Parse(path)
		if err != nil {
			log.ErrorC("Failed to create URL for ImportAPI call", err, logData)
			return nil, 0, err
		}
		path = URL.String()
		logData["URL"] = path
	} else {
		// TODO improve:  exponential backoff
		time.Sleep(time.Duration(attempts) * 10 * time.Second)
	}
	var req *http.Request
	var err error

	if payload != nil && method != "GET" {
		req, err = http.NewRequest(method, path, bytes.NewReader(payload.([]byte)))
		req.Header.Add("Content-type", "application/json")
		logData["payload"] = string(payload.([]byte))
	} else {
		req, err = http.NewRequest(method, path, nil)

		if payload != nil && method == "GET" {
			req.URL.RawQuery = payload.(url.Values).Encode()
			logData["payload"] = payload.(url.Values)
		}
	}
	// check req, above, didn't error
	if err != nil {
		log.ErrorC("Failed to create request for ImportAPI", err, logData)
		return nil, 0, err
	}

	req.Header.Set("Internal-token", AuthToken)
	resp, err := api.Client.Do(req)
	if err != nil {
		log.ErrorC("Failed to action ImportAPI", err, logData)
		if attempts < api.MaxRetries {
			return api.callImportAPI(method, path, attempts+1, payload)
		}
		return nil, 0, err
	}

	logData["httpCode"] = resp.StatusCode
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= 300 {
		log.Debug("unexpected status code from API", logData)
	}

	defer resp.Body.Close()
	jsonBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.ErrorC("Failed to read body from ImportAPI", err, logData)
		if attempts < api.MaxRetries {
			return api.callImportAPI(method, path, attempts+1, payload)
		}
		return nil, resp.StatusCode, err
	}
	return jsonBody, resp.StatusCode, nil
}
