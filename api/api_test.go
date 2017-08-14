package api

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

var (
	client = &http.Client{}
)

type MockedHTTPResponse struct {
	StatusCode int
	Body       string
}

func getMockImportAPI(expectRequest http.Request, mockedHTTPResponse MockedHTTPResponse) (*ImportAPI, error) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != expectRequest.Method {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("unexpected HTTP method used"))
			return
		}
		w.WriteHeader(mockedHTTPResponse.StatusCode)
		fmt.Fprintln(w, mockedHTTPResponse.Body)
	}))
	return New(client, ts.URL)
}

func TestGetInstance(t *testing.T) {
	instanceID := "iid0"
	Convey("When no import-instance is returned", t, func() {
		mockedAPI, _ := getMockImportAPI(http.Request{Method: "GET"}, MockedHTTPResponse{StatusCode: 200, Body: ""})
		instance, err := mockedAPI.GetInstance(instanceID)
		So(err, ShouldNotBeNil)
		So(instance, ShouldResemble, Instance{})
	})

	Convey("When bad json is returned", t, func() {
		mockedAPI, _ := getMockImportAPI(http.Request{Method: "GET"}, MockedHTTPResponse{StatusCode: 200, Body: "oops"})
		_, err := mockedAPI.GetInstance(instanceID)
		So(err, ShouldNotBeNil)
	})

	Convey("When server error is returned", t, func() {
		mockedAPI, _ := getMockImportAPI(http.Request{Method: "GET"}, MockedHTTPResponse{StatusCode: 500, Body: "dnm"})
		_, err := mockedAPI.GetInstance(instanceID)
		So(err, ShouldNotBeNil)
	})

	Convey("When a single import-instance is returned", t, func() {
		mockedAPI, _ := getMockImportAPI(http.Request{Method: "GET"}, MockedHTTPResponse{StatusCode: 200, Body: `{"instance_id":"iid","total_observations":1122,"total_inserted_observations":2233,"job":{"id":"jid1","link":"http://jid1"},"state":"created"}`})
		instance, err := mockedAPI.GetInstance(instanceID)
		So(err, ShouldBeNil)
		So(instance, ShouldResemble, Instance{
			State:                     "created",
			InstanceID:                "iid",
			NumberOfObservations:      1122,
			TotalInsertedObservations: 2233,
			Job: IDLink{
				ID:   "jid1",
				Link: "http://jid1",
			},
		})
	})
}

func TestGetInstances(t *testing.T) {
	Convey("When no import-instances are returned", t, func() {
		mockedAPI, _ := getMockImportAPI(http.Request{Method: "GET"}, MockedHTTPResponse{StatusCode: 200, Body: "[]"})
		instances, err := mockedAPI.GetInstances(nil)
		So(err, ShouldBeNil)
		So(instances, ShouldBeEmpty)
	})

	Convey("When bad json is returned", t, func() {
		mockedAPI, _ := getMockImportAPI(http.Request{Method: "GET"}, MockedHTTPResponse{StatusCode: 200, Body: "oops"})
		_, err := mockedAPI.GetInstances(nil)
		So(err, ShouldNotBeNil)
	})

	Convey("When server error is returned", t, func() {
		mockedAPI, _ := getMockImportAPI(http.Request{Method: "GET"}, MockedHTTPResponse{StatusCode: 500, Body: "[]"})
		_, err := mockedAPI.GetInstances(nil)
		So(err, ShouldNotBeNil)
	})

	Convey("When a single import-instance is returned", t, func() {
		mockedAPI, _ := getMockImportAPI(http.Request{Method: "GET"}, MockedHTTPResponse{StatusCode: 200, Body: `[{"instance_id":"iid","total_observations":1122}]`})
		instances, err := mockedAPI.GetInstances(nil)
		So(err, ShouldBeNil)
		So(instances, ShouldResemble, []Instance{Instance{InstanceID: "iid", NumberOfObservations: 1122}})
	})

	Convey("When multiple import-instances are returned", t, func() {
		mockedAPI, _ := getMockImportAPI(http.Request{Method: "GET"}, MockedHTTPResponse{StatusCode: 200, Body: `[{"instance_id":"iid","total_observations":1122},{"instance_id":"iid2","total_observations":2234}]`})
		instances, err := mockedAPI.GetInstances(nil)
		So(err, ShouldBeNil)
		So(instances, ShouldResemble, []Instance{
			Instance{
				InstanceID:           "iid",
				NumberOfObservations: 1122,
			},
			Instance{
				InstanceID:           "iid2",
				NumberOfObservations: 2234,
			},
		})
	})
}

func TestUpdateInstanceWithNewInserts(t *testing.T) {
	instanceID := "iid0"
	Convey("When bad request is returned", t, func() {
		mockedAPI, _ := getMockImportAPI(http.Request{Method: "PUT"}, MockedHTTPResponse{StatusCode: 400, Body: ""})
		err := mockedAPI.UpdateInstanceWithNewInserts(instanceID, 1234)
		So(err, ShouldNotBeNil)
	})

	Convey("When server error is returned", t, func() {
		mockedAPI, _ := getMockImportAPI(http.Request{Method: "PUT"}, MockedHTTPResponse{StatusCode: 500, Body: "dnm"})
		err := mockedAPI.UpdateInstanceWithNewInserts(instanceID, 1234)
		So(err, ShouldNotBeNil)
	})

	Convey("When a single import-instance is returned", t, func() {
		mockedAPI, _ := getMockImportAPI(http.Request{Method: "PUT"}, MockedHTTPResponse{StatusCode: 200, Body: ""})
		err := mockedAPI.UpdateInstanceWithNewInserts(instanceID, 1234)
		So(err, ShouldBeNil)
	})
}

func TestUpdateInstanceState(t *testing.T) {
	instanceID := "iid0"
	Convey("When bad request is returned", t, func() {
		mockedAPI, _ := getMockImportAPI(http.Request{Method: "PUT"}, MockedHTTPResponse{StatusCode: 400, Body: ""})
		err := mockedAPI.UpdateInstanceState(instanceID, "newState")
		So(err, ShouldNotBeNil)
	})

	Convey("When server error is returned", t, func() {
		mockedAPI, _ := getMockImportAPI(http.Request{Method: "PUT"}, MockedHTTPResponse{StatusCode: 500, Body: "dnm"})
		err := mockedAPI.UpdateInstanceState(instanceID, "newState")
		So(err, ShouldNotBeNil)
	})

	Convey("When a single import-instance is returned", t, func() {
		mockedAPI, _ := getMockImportAPI(http.Request{Method: "PUT"}, MockedHTTPResponse{StatusCode: 200, Body: ""})
		err := mockedAPI.UpdateInstanceState(instanceID, "newState")
		So(err, ShouldBeNil)
	})
}

func TestGetImportJob(t *testing.T) {
	jobID := "jid1"
	jobJSON := `{"job_id":"` + jobID + `","instances":[{"id":"iid1","link":"iid1link"}]}`
	jobMultiInstJSON := `{"job_id":"` + jobID + `","instances":[{"id":"iid1","link":"iid1link"},{"id":"iid2","link":"iid2link"}]}`

	Convey("When no import-job is returned", t, func() {
		mockedAPI, _ := getMockImportAPI(http.Request{Method: "GET"}, MockedHTTPResponse{StatusCode: 404, Body: ""})
		job, err := mockedAPI.GetImportJob(jobID)
		So(err, ShouldBeNil)
		So(job, ShouldResemble, ImportJob{})
	})

	Convey("When bad json is returned", t, func() {
		mockedAPI, _ := getMockImportAPI(http.Request{Method: "GET"}, MockedHTTPResponse{StatusCode: 200, Body: "oops"})
		_, err := mockedAPI.GetImportJob(jobID)
		So(err, ShouldNotBeNil)
	})

	Convey("When server error is returned", t, func() {
		mockedAPI, _ := getMockImportAPI(http.Request{Method: "GET"}, MockedHTTPResponse{StatusCode: 500, Body: "[]"})
		_, err := mockedAPI.GetImportJob(jobID)
		So(err, ShouldNotBeNil)
	})

	Convey("When a single-instance import-job is returned", t, func() {
		mockedAPI, _ := getMockImportAPI(http.Request{Method: "GET"}, MockedHTTPResponse{StatusCode: 200, Body: jobJSON})
		job, err := mockedAPI.GetImportJob(jobID)
		So(err, ShouldBeNil)
		So(job, ShouldResemble, ImportJob{JobID: jobID, Instances: []IDLink{IDLink{ID: "iid1", Link: "iid1link"}}})
	})

	Convey("When a multiple-instance import-job is returned", t, func() {
		mockedAPI, _ := getMockImportAPI(http.Request{Method: "GET"}, MockedHTTPResponse{StatusCode: 200, Body: jobMultiInstJSON})
		job, err := mockedAPI.GetImportJob(jobID)
		So(err, ShouldBeNil)
		So(job, ShouldResemble, ImportJob{JobID: jobID, Instances: []IDLink{
			IDLink{ID: "iid1", Link: "iid1link"},
			IDLink{ID: "iid2", Link: "iid2link"},
		}})
	})
}

func TestUpdateImportJobState(t *testing.T) {
	jobID := "jid0"
	Convey("When bad request is returned", t, func() {
		mockedAPI, _ := getMockImportAPI(http.Request{Method: "PUT"}, MockedHTTPResponse{StatusCode: 400, Body: ""})
		err := mockedAPI.UpdateImportJobState(jobID, "newState")
		So(err, ShouldNotBeNil)
	})

	Convey("When server error is returned", t, func() {
		mockedAPI, _ := getMockImportAPI(http.Request{Method: "PUT"}, MockedHTTPResponse{StatusCode: 500, Body: "dnm"})
		err := mockedAPI.UpdateImportJobState(jobID, "newState")
		So(err, ShouldNotBeNil)
	})

	Convey("When a single import-instance is returned", t, func() {
		mockedAPI, _ := getMockImportAPI(http.Request{Method: "PUT"}, MockedHTTPResponse{StatusCode: 200, Body: ""})
		err := mockedAPI.UpdateImportJobState(jobID, "newState")
		So(err, ShouldBeNil)
	})
}
