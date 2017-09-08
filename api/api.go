package api

import (
	"bytes"
	"io/ioutil"
	"net/http"
	"net/url"
	"time"

	"github.com/ONSdigital/go-ns/log"
)

var (
	maxRetries = 5
)

func callAPI(
	client *http.Client,
	method, path, authToken string,
	maxRetries, attempts int,
	payload interface{}) ([]byte, int, error) {

	logData := log.Data{"url": path, "method": method, "attempts": attempts}

	if attempts == 0 {
		URL, err := url.Parse(path)
		if err != nil {
			log.ErrorC("Failed to create url for ImportAPI call", err, logData)
			return nil, 0, err
		}
		path = URL.String()
		logData["url"] = path
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

	req.Header.Set("Internal-token", authToken)
	resp, err := client.Do(req)
	if err != nil {
		log.ErrorC("Failed to action ImportAPI", err, logData)
		if attempts < maxRetries {
			return callAPI(client, method, path, authToken, maxRetries, attempts+1, payload)
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
			return callAPI(client, method, path, authToken, maxRetries, attempts+1, payload)
		}
		return nil, resp.StatusCode, err
	}
	return jsonBody, resp.StatusCode, nil
}
