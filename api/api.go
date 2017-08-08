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

var AuthToken string

type Instance struct {
	InstanceID                string `json:"instance_id"`
	NumberOfObservations      int64  `json:"total_observations"`
	TotalInsertedObservations int64  `json:"total_inserted_observations,omitempty"`
	Job                       JobRef `json:"job"`
	State                     string `json:"state"`
}

type InstanceRef struct {
	InstanceID string `json:"id"`
	Link       string `json:"link"`
}

type JobRef struct {
	ID   string `json:"id"`
	Link string `json:"link"`
}

type ImportJob struct {
	JobID     string        `json:"job_id"`
	Instances []InstanceRef `json:"instances"`
}

var (
	maxRetries = 5
)

func GetInstance(client *http.Client, importAPIURL string, instanceID string) (Instance, error) {
	path := importAPIURL + "/instances/" + instanceID
	logData := log.Data{"path": path, "instanceID": instanceID}
	jsonBody, httpCode, err := Get(client, path, 0, nil)
	logData["httpCode"] = httpCode
	if httpCode >= 300 {
		err = errors.New("Bad response while getting import instance")
	}
	if err != nil {
		log.ErrorC("Failed to get import instance", err, logData)
		return Instance{}, err
	}
	logData["jsonBody"] = jsonBody

	var instance Instance
	if err := json.Unmarshal(jsonBody, &instance); err != nil {
		log.ErrorC("Failed to parse json message", err, logData)
		return Instance{}, err
	}
	return instance, nil
}

func GetInstances(client *http.Client, importAPIURL string, vars url.Values) ([]Instance, error) {
	//return []Instance{}, nil // XXX TODO XXX
	path := importAPIURL + "/instances"
	logData := log.Data{"path": path}
	jsonBody, httpCode, err := Get(client, path, 0, vars)
	logData["httpCode"] = httpCode
	if httpCode >= 300 {
		err = errors.New("Bad response while getting import instance list")
	}
	if err != nil {
		log.ErrorC("Failed to get import instance list", err, logData)
		log.Error(err, logData)
		return nil, err
	}
	logData["jsonBody"] = jsonBody

	var instances []Instance
	if err := json.Unmarshal(jsonBody, &instances); err != nil {
		log.ErrorC("Failed to parse json message", err, logData)
		return nil, err
	}
	return instances, nil
}

func UpdateInstanceWithNewInserts(client *http.Client, importAPIURL, instanceID string, observationsInserted int32) error {
	path := importAPIURL + "/instances/" + instanceID + "/inserted_observations/" + strconv.FormatInt(int64(observationsInserted), 10)
	logData := log.Data{"URL": path}
	jsonBody, httpCode, err := Put(client, path, 0, nil)
	logData["httpCode"] = httpCode
	logData["jsonBytes"] = jsonBody
	if err != nil {
		log.ErrorC("Failed to PUT import instance", err, logData)
		return err
	}
	log.Info("ook", logData)
	return nil
}

func UpdateInstanceState(client *http.Client, importAPIURL, instanceID string, newState string) error {
	path := importAPIURL + "/instances/" + instanceID
	logData := log.Data{"URL": path}
	jsonUpload := []byte(`{"instance_id":"` + instanceID + `","state":"` + newState + `"}`)
	logData["jsonUpload"] = jsonUpload
	jsonResult, httpCode, err := Put(client, path, 0, jsonUpload)
	logData["httpCode"] = httpCode
	logData["jsonResult"] = jsonResult
	if err != nil {
		log.ErrorC("Failed to PUT instance state", err, logData)
		return err
	}
	return nil
}

func GetImportJob(client *http.Client, importAPIURL string, importJobID string) (ImportJob, error) {
	path := importAPIURL + "/jobs/" + importJobID
	logData := log.Data{"path": path, "importJobID": importJobID}
	jsonBody, httpCode, err := Get(client, path, 0, nil)
	logData["httpCode"] = httpCode
	if err == nil && httpCode >= 300 {
		err = errors.New("Bad httpCode")
	}
	if err != nil {
		log.ErrorC("Failed to get import job", err, logData)
		return ImportJob{}, err
	}
	logData["jsonBody"] = string(jsonBody)

	var importJob ImportJob
	if err := json.Unmarshal(jsonBody, &importJob); err != nil {
		log.ErrorC("Failed to parse json message", err, logData)
		return ImportJob{}, err
	}
	logData["importJob"] = importJob
	log.Debug("get import job", logData)

	return importJob, nil
}

/*
func GetImportJobList(client *http.Client, importAPIURL string, vars url.Values) ([]ImportJob, error) {
	path := importAPIURL + "/jobs"
	logData := log.Data{"path": path}
	jsonBody, httpCode, err := Get(client, path, 0, vars)
	logData["httpCode"] = httpCode
	if httpCode >= 300 {
		err = errors.New("Bad response while getting import job list")
	}
	if err != nil {
		log.ErrorC("Failed to get import job list", err, logData)
		return nil, err
	}
	logData["jsonBody"] = jsonBody
	var importJobs []ImportJob
	if err := json.Unmarshal(jsonBody, &importJobs); err != nil {
		log.ErrorC("Failed to parse json message", err, logData)
		return nil, err
	}
	return importJobs, nil
}
*/

func UpdateImportJobState(client *http.Client, importAPIURL, jobID string, newState string) error {
	path := importAPIURL + "/jobs/" + jobID
	logData := log.Data{"URL": path}
	jsonUpload := []byte(`{"job_id":"` + jobID + `","state":"` + newState + `"}`)
	logData["jsonUpload"] = jsonUpload
	jsonResult, httpCode, err := Put(client, path, 0, jsonUpload)
	logData["httpCode"] = httpCode
	logData["jsonResult"] = jsonResult
	if err != nil {
		log.ErrorC("Failed to PUT import job state", err, logData)
		return err
	}
	return nil
}

func Get(client *http.Client, path string, attempts int, vars url.Values) ([]byte, int, error) {
	return callImportAPI(client, "GET", path, attempts, vars)
}

func Put(client *http.Client, path string, attempts int, payload []byte) ([]byte, int, error) {
	return callImportAPI(client, "PUT", path, attempts, payload)
}

// callImportAPI contacts the Import API returns the json body (action = PUT, GET, POST, ...)
func callImportAPI(client *http.Client, method, path string, attempts int, payload interface{}) ([]byte, int, error) {
	logData := log.Data{"URL": path, "method": method, "attempts": attempts}

	if attempts == 0 {
		URL, err := url.Parse(path)
		if err != nil {
			log.ErrorC("Failed to create URL for ImportAPI call", err, logData)
			return nil, 0, err
		}
		path = URL.String()
		logData["URL"] = path
	} else if attempts > 0 {
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
	resp, err := client.Do(req)
	if err != nil {
		log.ErrorC("Failed to action ImportAPI", err, logData)
		if attempts < maxRetries {
			return callImportAPI(client, method, path, attempts+1, payload)
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
		if attempts < maxRetries {
			return callImportAPI(client, method, path, attempts+1, payload)
		}
		return nil, resp.StatusCode, err
	}
	return jsonBody, resp.StatusCode, nil
}
