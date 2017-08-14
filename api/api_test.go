package api

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

var (
	jobs = []ImportJob{}
	ts   = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, `[]`)
	}))
	//defer ts.Close()
	client = &http.Client{}
)

type MockedHttpResponse struct {
	StatusCode int
	Body       string
}

func getMockImportAPI(expectRequest http.Request, mockedHttpResponse MockedHttpResponse) *httptest.Server {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != expectRequest.Method {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("unexpected HTTP method used"))
			return
		}
		w.WriteHeader(mockedHttpResponse.StatusCode)
		fmt.Fprintln(w, mockedHttpResponse.Body)
	}))
	return ts
}

func TestGetInstance(t *testing.T) {
	instanceID := "iid0"
	Convey("When no import-instance is returned", t, func() {
		mockedAPI := getMockImportAPI(http.Request{Method: "GET"}, MockedHttpResponse{StatusCode: 200, Body: ""})
		instance, err := GetInstance(client, mockedAPI.URL, instanceID)
		So(err, ShouldNotBeNil)
		So(instance, ShouldResemble, Instance{})
	})

	Convey("When bad json is returned", t, func() {
		mockedAPI := getMockImportAPI(http.Request{Method: "GET"}, MockedHttpResponse{StatusCode: 200, Body: "oops"})
		_, err := GetInstance(client, mockedAPI.URL, instanceID)
		So(err, ShouldNotBeNil)
	})

	Convey("When server error is returned", t, func() {
		mockedAPI := getMockImportAPI(http.Request{Method: "GET"}, MockedHttpResponse{StatusCode: 500, Body: "dnm"})
		_, err := GetInstance(client, mockedAPI.URL, instanceID)
		So(err, ShouldNotBeNil)
	})

	Convey("When a single import-instance is returned", t, func() {
		mockedAPI := getMockImportAPI(http.Request{Method: "GET"}, MockedHttpResponse{StatusCode: 200, Body: `{"instance_id":"iid","total_observations":1122,"total_inserted_observations":2233,"job":{"id":"jid1","link":"http://jid1"},"state":"created"}`})
		instance, err := GetInstance(client, mockedAPI.URL, instanceID)
		So(err, ShouldBeNil)
		So(instance, ShouldResemble, Instance{
			State:                     "created",
			InstanceID:                "iid",
			NumberOfObservations:      1122,
			TotalInsertedObservations: 2233,
			Job: JobRef{
				ID:   "jid1",
				Link: "http://jid1",
			},
		})
	})
}

func TestGetInstances(t *testing.T) {
	Convey("When no import-instances are returned", t, func() {
		mockedAPI := getMockImportAPI(http.Request{Method: "GET"}, MockedHttpResponse{StatusCode: 200, Body: "[]"})
		instances, err := GetInstances(client, mockedAPI.URL, nil)
		So(err, ShouldBeNil)
		So(instances, ShouldBeEmpty)
	})

	Convey("When bad json is returned", t, func() {
		mockedAPI := getMockImportAPI(http.Request{Method: "GET"}, MockedHttpResponse{StatusCode: 200, Body: "oops"})
		_, err := GetInstances(client, mockedAPI.URL, nil)
		So(err, ShouldNotBeNil)
	})

	Convey("When server error is returned", t, func() {
		mockedAPI := getMockImportAPI(http.Request{Method: "GET"}, MockedHttpResponse{StatusCode: 500, Body: "[]"})
		_, err := GetInstances(client, mockedAPI.URL, nil)
		So(err, ShouldNotBeNil)
	})

	Convey("When a single import-instance is returned", t, func() {
		mockedAPI := getMockImportAPI(http.Request{Method: "GET"}, MockedHttpResponse{StatusCode: 200, Body: `[{"instance_id":"iid","total_observations":1122}]`})
		instances, err := GetInstances(client, mockedAPI.URL, nil)
		So(err, ShouldBeNil)
		So(instances, ShouldResemble, []Instance{Instance{InstanceID: "iid", NumberOfObservations: 1122}})
	})

	Convey("When multiple import-instances are returned", t, func() {
		mockedAPI := getMockImportAPI(http.Request{Method: "GET"}, MockedHttpResponse{StatusCode: 200, Body: `[{"instance_id":"iid","total_observations":1122},{"instance_id":"iid2","total_observations":2234}]`})
		instances, err := GetInstances(client, mockedAPI.URL, nil)
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
		mockedAPI := getMockImportAPI(http.Request{Method: "PUT"}, MockedHttpResponse{StatusCode: 400, Body: ""})
		err := UpdateInstanceWithNewInserts(client, mockedAPI.URL, instanceID, 1234)
		So(err, ShouldNotBeNil)
	})

	Convey("When server error is returned", t, func() {
		mockedAPI := getMockImportAPI(http.Request{Method: "PUT"}, MockedHttpResponse{StatusCode: 500, Body: "dnm"})
		err := UpdateInstanceWithNewInserts(client, mockedAPI.URL, instanceID, 1234)
		So(err, ShouldNotBeNil)
	})

	Convey("When a single import-instance is returned", t, func() {
		mockedAPI := getMockImportAPI(http.Request{Method: "PUT"}, MockedHttpResponse{StatusCode: 200, Body: ""})
		err := UpdateInstanceWithNewInserts(client, mockedAPI.URL, instanceID, 1234)
		So(err, ShouldBeNil)
	})
}

func TestUpdateInstanceState(t *testing.T) {
	instanceID := "iid0"
	Convey("When bad request is returned", t, func() {
		mockedAPI := getMockImportAPI(http.Request{Method: "PUT"}, MockedHttpResponse{StatusCode: 400, Body: ""})
		err := UpdateInstanceState(client, mockedAPI.URL, instanceID, "newState")
		So(err, ShouldNotBeNil)
	})

	Convey("When server error is returned", t, func() {
		mockedAPI := getMockImportAPI(http.Request{Method: "PUT"}, MockedHttpResponse{StatusCode: 500, Body: "dnm"})
		err := UpdateInstanceState(client, mockedAPI.URL, instanceID, "newState")
		So(err, ShouldNotBeNil)
	})

	Convey("When a single import-instance is returned", t, func() {
		mockedAPI := getMockImportAPI(http.Request{Method: "PUT"}, MockedHttpResponse{StatusCode: 200, Body: ""})
		err := UpdateInstanceState(client, mockedAPI.URL, instanceID, "newState")
		So(err, ShouldBeNil)
	})
}

func TestGetImportJob(t *testing.T) {
	jobID := "jid1"
	jobJson := `{"job_id":"` + jobID + `","instances":[{"id":"iid1","link":"iid1link"}]}`
	jobMultiInstJson := `{"job_id":"` + jobID + `","instances":[{"id":"iid1","link":"iid1link"},{"id":"iid2","link":"iid2link"}]}`

	Convey("When no import-job is returned", t, func() {
		mockedAPI := getMockImportAPI(http.Request{Method: "GET"}, MockedHttpResponse{StatusCode: 404, Body: ""})
		job, err := GetImportJob(client, mockedAPI.URL, jobID)
		So(err, ShouldBeNil)
		So(job, ShouldResemble, ImportJob{})
	})

	Convey("When bad json is returned", t, func() {
		mockedAPI := getMockImportAPI(http.Request{Method: "GET"}, MockedHttpResponse{StatusCode: 200, Body: "oops"})
		_, err := GetImportJob(client, mockedAPI.URL, jobID)
		So(err, ShouldNotBeNil)
	})

	Convey("When server error is returned", t, func() {
		mockedAPI := getMockImportAPI(http.Request{Method: "GET"}, MockedHttpResponse{StatusCode: 500, Body: "[]"})
		_, err := GetImportJob(client, mockedAPI.URL, jobID)
		So(err, ShouldNotBeNil)
	})

	Convey("When a single-instance import-job is returned", t, func() {
		mockedAPI := getMockImportAPI(http.Request{Method: "GET"}, MockedHttpResponse{StatusCode: 200, Body: jobJson})
		job, err := GetImportJob(client, mockedAPI.URL, jobID)
		So(err, ShouldBeNil)
		So(job, ShouldResemble, ImportJob{JobID: jobID, Instances: []InstanceRef{InstanceRef{InstanceID: "iid1", Link: "iid1link"}}})
	})

	Convey("When a multiple-instance import-job is returned", t, func() {
		mockedAPI := getMockImportAPI(http.Request{Method: "GET"}, MockedHttpResponse{StatusCode: 200, Body: jobMultiInstJson})
		job, err := GetImportJob(client, mockedAPI.URL, jobID)
		So(err, ShouldBeNil)
		So(job, ShouldResemble, ImportJob{JobID: jobID, Instances: []InstanceRef{
			InstanceRef{InstanceID: "iid1", Link: "iid1link"},
			InstanceRef{InstanceID: "iid2", Link: "iid2link"},
		}})
	})
}

func TestUpdateImportJobState(t *testing.T) {
	jobID := "jid0"
	Convey("When bad request is returned", t, func() {
		mockedAPI := getMockImportAPI(http.Request{Method: "PUT"}, MockedHttpResponse{StatusCode: 400, Body: ""})
		err := UpdateImportJobState(client, mockedAPI.URL, jobID, "newState")
		So(err, ShouldNotBeNil)
	})

	Convey("When server error is returned", t, func() {
		mockedAPI := getMockImportAPI(http.Request{Method: "PUT"}, MockedHttpResponse{StatusCode: 500, Body: "dnm"})
		err := UpdateImportJobState(client, mockedAPI.URL, jobID, "newState")
		So(err, ShouldNotBeNil)
	})

	Convey("When a single import-instance is returned", t, func() {
		mockedAPI := getMockImportAPI(http.Request{Method: "PUT"}, MockedHttpResponse{StatusCode: 200, Body: ""})
		err := UpdateImportJobState(client, mockedAPI.URL, jobID, "newState")
		So(err, ShouldBeNil)
	})
}
