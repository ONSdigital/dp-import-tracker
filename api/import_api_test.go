package api

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ONSdigital/go-ns/rhttp"
	. "github.com/smartystreets/goconvey/convey"
)

var (
	client = &rhttp.Client{HTTPClient: &http.Client{}}
)

type MockedHTTPResponse struct {
	StatusCode int
	Body       string
}

func getMockImportAPI(expectRequest http.Request, mockedHTTPResponse MockedHTTPResponse) *ImportAPI {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != expectRequest.Method {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("unexpected HTTP method used"))
			return
		}
		w.WriteHeader(mockedHTTPResponse.StatusCode)
		fmt.Fprintln(w, mockedHTTPResponse.Body)
	}))
	return NewImportAPI(client, ts.URL, "345")
}

func TestGetImportJob(t *testing.T) {
	jobID := "jid1"
	jobJSON := `{"job_id":"` + jobID + `","instances":[{"id":"iid1","link":"iid1link"}]}`
	jobMultiInstJSON := `{"job_id":"` + jobID + `","instances":[{"id":"iid1","link":"iid1link"},{"id":"iid2","link":"iid2link"}]}`

	Convey("When no import-job is returned", t, func() {
		mockedAPI := getMockImportAPI(http.Request{Method: "GET"}, MockedHTTPResponse{StatusCode: 404, Body: ""})
		job, err := mockedAPI.GetImportJob(jobID)
		So(err, ShouldBeNil)
		So(job, ShouldResemble, ImportJob{})
	})

	Convey("When bad json is returned", t, func() {
		mockedAPI := getMockImportAPI(http.Request{Method: "GET"}, MockedHTTPResponse{StatusCode: 200, Body: "oops"})
		_, err := mockedAPI.GetImportJob(jobID)
		So(err, ShouldNotBeNil)
	})

	Convey("When server error is returned", t, func() {
		mockedAPI := getMockImportAPI(http.Request{Method: "GET"}, MockedHTTPResponse{StatusCode: 500, Body: "[]"})
		_, err := mockedAPI.GetImportJob(jobID)
		So(err, ShouldNotBeNil)
	})

	Convey("When a single-instance import-job is returned", t, func() {
		mockedAPI := getMockImportAPI(http.Request{Method: "GET"}, MockedHTTPResponse{StatusCode: 200, Body: jobJSON})
		job, err := mockedAPI.GetImportJob(jobID)
		So(err, ShouldBeNil)
		So(job, ShouldResemble, ImportJob{JobID: jobID, Instances: []InstanceLink{InstanceLink{ID: "iid1", Link: "iid1link"}}})
	})

	Convey("When a multiple-instance import-job is returned", t, func() {
		mockedAPI := getMockImportAPI(http.Request{Method: "GET"}, MockedHTTPResponse{StatusCode: 200, Body: jobMultiInstJSON})
		job, err := mockedAPI.GetImportJob(jobID)
		So(err, ShouldBeNil)
		So(job, ShouldResemble, ImportJob{JobID: jobID, Instances: []InstanceLink{
			InstanceLink{ID: "iid1", Link: "iid1link"},
			InstanceLink{ID: "iid2", Link: "iid2link"},
		}})
	})
}

func TestUpdateImportJobState(t *testing.T) {
	jobID := "jid0"
	Convey("When bad request is returned", t, func() {
		mockedAPI := getMockImportAPI(http.Request{Method: "PUT"}, MockedHTTPResponse{StatusCode: 400, Body: ""})
		err := mockedAPI.UpdateImportJobState(jobID, "newState")
		So(err, ShouldNotBeNil)
	})

	Convey("When server error is returned", t, func() {
		mockedAPI := getMockImportAPI(http.Request{Method: "PUT"}, MockedHTTPResponse{StatusCode: 500, Body: "dnm"})
		err := mockedAPI.UpdateImportJobState(jobID, "newState")
		So(err, ShouldNotBeNil)
	})

	Convey("When a single import-instance is returned", t, func() {
		mockedAPI := getMockImportAPI(http.Request{Method: "PUT"}, MockedHTTPResponse{StatusCode: 200, Body: ""})
		err := mockedAPI.UpdateImportJobState(jobID, "newState")
		So(err, ShouldBeNil)
	})
}
