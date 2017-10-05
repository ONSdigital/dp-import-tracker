package api

import (
	"context"
	"net/http"
	"testing"

	"fmt"
	"net/http/httptest"

	. "github.com/smartystreets/goconvey/convey"
)

var ctx = context.Background()

func TestGetInstance(t *testing.T) {
	instanceID := "iid0"
	Convey("When no import-instance is returned", t, func() {
		mockedAPI := getMockDatasetAPI(http.Request{Method: "GET"}, MockedHTTPResponse{StatusCode: 200, Body: ""})
		instance, err := mockedAPI.GetInstance(ctx, instanceID)
		So(err, ShouldNotBeNil)
		So(instance, ShouldResemble, Instance{})
	})

	Convey("When bad json is returned", t, func() {
		mockedAPI := getMockDatasetAPI(http.Request{Method: "GET"}, MockedHTTPResponse{StatusCode: 200, Body: "oops"})
		_, err := mockedAPI.GetInstance(ctx, instanceID)
		So(err, ShouldNotBeNil)
	})

	Convey("When server error is returned", t, func() {
		mockedAPI := getMockDatasetAPI(http.Request{Method: "GET"}, MockedHTTPResponse{StatusCode: 500, Body: "dnm"})
		_, err := mockedAPI.GetInstance(ctx, instanceID)
		So(err, ShouldNotBeNil)
	})

	Convey("When a single import-instance is returned", t, func() {
		mockedAPI := getMockDatasetAPI(http.Request{Method: "GET"},
			MockedHTTPResponse{StatusCode: 200,
				Body: `{"id":"iid","total_observations":1122,"total_inserted_observations":2233,"links":{"job":{"id":"jid1","href":"http://jid1"}},"state":"created"}`})

		instance, err := mockedAPI.GetInstance(ctx, instanceID)
		So(err, ShouldBeNil)
		So(instance, ShouldResemble, Instance{
			State:                     "created",
			InstanceID:                "iid",
			NumberOfObservations:      1122,
			TotalInsertedObservations: 2233,
			Links: InstanceLinks{
				Job: JobLinks{
					ID:   "jid1",
					HRef: "http://jid1",
				},
			},
		})
	})
}

func TestGetInstances(t *testing.T) {
	Convey("When no import-instances are returned", t, func() {
		mockedAPI := getMockDatasetAPI(http.Request{Method: "GET"}, MockedHTTPResponse{StatusCode: 200, Body: `{"items":[]}`})
		instances, err := mockedAPI.GetInstances(ctx, nil)
		So(err, ShouldBeNil)
		So(instances, ShouldBeEmpty)
	})

	Convey("When bad json is returned", t, func() {
		mockedAPI := getMockDatasetAPI(http.Request{Method: "GET"}, MockedHTTPResponse{StatusCode: 200, Body: "oops"})
		_, err := mockedAPI.GetInstances(ctx, nil)
		So(err, ShouldNotBeNil)
	})

	Convey("When server error is returned", t, func() {
		mockedAPI := getMockDatasetAPI(http.Request{Method: "GET"}, MockedHTTPResponse{StatusCode: 500, Body: `{"items":[]}`})
		_, err := mockedAPI.GetInstances(ctx, nil)
		So(err, ShouldNotBeNil)
	})

	Convey("When a single import-instance is returned", t, func() {
		mockedAPI := getMockDatasetAPI(http.Request{Method: "GET"}, MockedHTTPResponse{StatusCode: 200, Body: `{"items":[{"id":"iid","total_observations":1122}]}`})
		instances, err := mockedAPI.GetInstances(ctx, nil)
		So(err, ShouldBeNil)
		So(instances, ShouldResemble, []Instance{Instance{InstanceID: "iid", NumberOfObservations: 1122}})
	})

	Convey("When multiple import-instances are returned", t, func() {
		mockedAPI := getMockDatasetAPI(
			http.Request{Method: "GET"},
			MockedHTTPResponse{StatusCode: 200,
				Body: `{"items":[{"id":"iid","total_observations":1122},{"id":"iid2","total_observations":2234}]}`})

		instances, err := mockedAPI.GetInstances(ctx, nil)
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
		mockedAPI := getMockDatasetAPI(http.Request{Method: "PUT"}, MockedHTTPResponse{StatusCode: 400, Body: ""})
		err := mockedAPI.UpdateInstanceWithNewInserts(ctx, instanceID, 1234)
		So(err, ShouldNotBeNil)
	})

	Convey("When server error is returned", t, func() {
		mockedAPI := getMockDatasetAPI(http.Request{Method: "PUT"}, MockedHTTPResponse{StatusCode: 500, Body: "dnm"})
		err := mockedAPI.UpdateInstanceWithNewInserts(ctx, instanceID, 1234)
		So(err, ShouldNotBeNil)
	})

	Convey("When a single import-instance is returned", t, func() {
		mockedAPI := getMockDatasetAPI(http.Request{Method: "PUT"}, MockedHTTPResponse{StatusCode: 200, Body: ""})
		err := mockedAPI.UpdateInstanceWithNewInserts(ctx, instanceID, 1234)
		So(err, ShouldBeNil)
	})
}

func TestUpdateInstanceState(t *testing.T) {
	instanceID := "iid0"
	Convey("When bad request is returned", t, func() {
		mockedAPI := getMockDatasetAPI(http.Request{Method: "PUT"}, MockedHTTPResponse{StatusCode: 400, Body: ""})
		err := mockedAPI.UpdateInstanceState(ctx, instanceID, "newState")
		So(err, ShouldNotBeNil)
	})

	Convey("When server error is returned", t, func() {
		mockedAPI := getMockDatasetAPI(http.Request{Method: "PUT"}, MockedHTTPResponse{StatusCode: 500, Body: "dnm"})
		err := mockedAPI.UpdateInstanceState(ctx, instanceID, "newState")
		So(err, ShouldNotBeNil)
	})

	Convey("When a single import-instance is returned", t, func() {
		mockedAPI := getMockDatasetAPI(http.Request{Method: "PUT"}, MockedHTTPResponse{StatusCode: 200, Body: ""})
		err := mockedAPI.UpdateInstanceState(ctx, instanceID, "newState")
		So(err, ShouldBeNil)
	})
}

func getMockDatasetAPI(expectRequest http.Request, mockedHTTPResponse MockedHTTPResponse) *DatasetAPI {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != expectRequest.Method {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("unexpected HTTP method used"))
			return
		}
		w.WriteHeader(mockedHTTPResponse.StatusCode)
		fmt.Fprintln(w, mockedHTTPResponse.Body)
	}))
	return NewDatasetAPI(client, ts.URL, "123")
}
