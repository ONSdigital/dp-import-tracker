package api

import (
	"bytes"
	"context"
	"io/ioutil"
	"net/http"
	"net/url"

	"github.com/ONSdigital/go-ns/common"
	"github.com/ONSdigital/go-ns/rchttp"
	"github.com/ONSdigital/log.go/log"
)

func callAPI(
	ctx context.Context,
	client *rchttp.Client,
	method, path, authToken string,
	payload interface{}) ([]byte, int, error,
) {

	logData := log.Data{"url": path, "method": method}

	URL, err := url.Parse(path)
	if err != nil {
		log.Event(ctx, "Failed to create url for API call", log.ERROR, log.Error(err), logData)
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
		log.Event(ctx, "Failed to create request for API", log.ERROR, log.Error(err), logData)
		return nil, 0, err
	}

	req.Header.Set(common.AuthHeaderKey, authToken)

	resp, err := client.Do(ctx, req)
	if err != nil {
		log.Event(ctx, "Failed to action API", log.ERROR, log.Error(err), logData)
		return nil, 0, err
	}

	logData["httpCode"] = resp.StatusCode
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= 300 {
		log.Event(ctx, "unexpected status code from API", log.INFO, logData)
	}

	defer func() {
		if err = resp.Body.Close(); err != nil {
			log.Event(ctx, "closing body", log.ERROR, log.Error(err), nil)
		}
	}()

	jsonBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Event(ctx, "Failed to read body from API", log.ERROR, log.Error(err), logData)
		return nil, resp.StatusCode, err
	}
	return jsonBody, resp.StatusCode, nil
}
