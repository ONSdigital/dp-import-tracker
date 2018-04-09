package api

import (
	"bytes"
	"context"
	"io/ioutil"
	"net/http"
	"net/url"

	"github.com/ONSdigital/go-ns/log"
	"github.com/ONSdigital/go-ns/rchttp"
)

const authorizationHeader = "Authorization"

func callAPI(
	ctx context.Context,
	client *rchttp.Client,
	method, path, authToken, datasetAPIAuthToken string,
	payload interface{}) ([]byte, int, error,
) {

	logData := log.Data{"url": path, "method": method}

	URL, err := url.Parse(path)
	if err != nil {
		log.ErrorC("Failed to create url for API call", err, logData)
		return nil, 0, err
	}
	path = URL.String()
	logData["url"] = path

	var req *http.Request

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
		log.ErrorC("Failed to create request for API", err, logData)
		return nil, 0, err
	}

	// TODO Remove `Internal-token` header, now uses "Authorization" header
	req.Header.Set("Internal-token", datasetAPIAuthToken)
	req.Header.Set(authorizationHeader, authToken)

	resp, err := client.Do(ctx, req)
	if err != nil {
		log.ErrorC("Failed to action API", err, logData)
		return nil, 0, err
	}

	logData["httpCode"] = resp.StatusCode
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= 300 {
		log.Debug("unexpected status code from API", logData)
	}

	defer func() {
		if err = resp.Body.Close(); err != nil {
			log.ErrorC("closing body", err, nil)
		}
	}()

	jsonBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.ErrorC("Failed to read body from API", err, logData)
		return nil, resp.StatusCode, err
	}
	return jsonBody, resp.StatusCode, nil
}
