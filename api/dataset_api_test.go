package api

import (
	"context"
	"net/http"
	"testing"

	"fmt"
	"net/http/httptest"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/ONSdigital/go-ns/rchttp"
)

var ctx = context.Background()

var (
	client = &rchttp.Client{HTTPClient: &http.Client{}}
)

type MockedHTTPResponse struct {
	StatusCode int
	Body       string
}

func TestGetInstance(t *testing.T) {
	instanceID := "iid0"
	Convey("When no import-instance is returned", t, func() {
		mockedAPI := getMockDatasetAPI(http.Request{Method: "GET"}, MockedHTTPResponse{StatusCode: 200, Body: ""})
		instance, isFatal, err := mockedAPI.GetInstance(ctx, instanceID)
		So(err, ShouldNotBeNil)
		So(instance, ShouldResemble, Instance{})
		So(isFatal, ShouldBeTrue)
	})

	Convey("When bad json is returned", t, func() {
		mockedAPI := getMockDatasetAPI(http.Request{Method: "GET"}, MockedHTTPResponse{StatusCode: 200, Body: "oops"})
		_, isFatal, err := mockedAPI.GetInstance(ctx, instanceID)
		So(err, ShouldNotBeNil)
		So(isFatal, ShouldBeTrue)
	})

	Convey("When server error is returned", t, func() {
		mockedAPI := getMockDatasetAPI(http.Request{Method: "GET"}, MockedHTTPResponse{StatusCode: 500, Body: "dnm"})
		_, isFatal, err := mockedAPI.GetInstance(ctx, instanceID)
		So(err, ShouldNotBeNil)
		So(isFatal, ShouldBeFalse)
	})

	Convey("When a single import-instance is returned", t, func() {
		mockedAPI := getMockDatasetAPI(http.Request{Method: "GET"},
			MockedHTTPResponse{StatusCode: 200,
				Body: `{"id":"iid","total_observations":1122,"total_inserted_observations":2233,"links":{"job":{"id":"jid1","href":"http://jid1"}},"state":"created"}`})

		instance, isFatal, err := mockedAPI.GetInstance(ctx, instanceID)
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
		So(isFatal, ShouldBeFalse)
	})
}

func TestGetInstances(t *testing.T) {
	Convey("When no import-instances are returned", t, func() {
		mockedAPI := getMockDatasetAPI(http.Request{Method: "GET"}, MockedHTTPResponse{StatusCode: 200, Body: `{"items":[]}`})
		instances, isFatal, err := mockedAPI.GetInstances(ctx, nil)
		So(err, ShouldBeNil)
		So(instances, ShouldBeEmpty)
		So(isFatal, ShouldBeFalse)
	})

	Convey("When bad json is returned", t, func() {
		mockedAPI := getMockDatasetAPI(http.Request{Method: "GET"}, MockedHTTPResponse{StatusCode: 200, Body: "oops"})
		_, isFatal, err := mockedAPI.GetInstances(ctx, nil)
		So(err, ShouldNotBeNil)
		So(isFatal, ShouldBeTrue)
	})

	Convey("When server error is returned", t, func() {
		mockedAPI := getMockDatasetAPI(http.Request{Method: "GET"}, MockedHTTPResponse{StatusCode: 500, Body: `{"items":[]}`})
		_, isFatal, err := mockedAPI.GetInstances(ctx, nil)
		So(err, ShouldNotBeNil)
		So(isFatal, ShouldBeFalse)
	})

	Convey("When a single import-instance is returned", t, func() {
		mockedAPI := getMockDatasetAPI(http.Request{Method: "GET"}, MockedHTTPResponse{StatusCode: 200, Body: `{"items":[{"id":"iid","total_observations":1122}]}`})
		instances, isFatal, err := mockedAPI.GetInstances(ctx, nil)
		So(err, ShouldBeNil)
		So(instances, ShouldResemble, []Instance{Instance{InstanceID: "iid", NumberOfObservations: 1122}})
		So(isFatal, ShouldBeFalse)
	})

	Convey("When multiple import-instances are returned", t, func() {
		mockedAPI := getMockDatasetAPI(
			http.Request{Method: "GET"},
			MockedHTTPResponse{StatusCode: 200,
				Body: `{"items":[{"id":"iid","total_observations":1122},{"id":"iid2","total_observations":2234}]}`})

		instances, isFatal, err := mockedAPI.GetInstances(ctx, nil)
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
		So(isFatal, ShouldBeFalse)
	})
}

func TestUpdateInstanceWithNewInserts(t *testing.T) {
	instanceID := "iid0"
	Convey("When bad request is returned", t, func() {
		mockedAPI := getMockDatasetAPI(http.Request{Method: "PUT"}, MockedHTTPResponse{StatusCode: 400, Body: ""})
		isFatal, err := mockedAPI.UpdateInstanceWithNewInserts(ctx, instanceID, 1234)
		So(err, ShouldNotBeNil)
		So(isFatal, ShouldBeTrue)
	})

	Convey("When server error is returned", t, func() {
		mockedAPI := getMockDatasetAPI(http.Request{Method: "PUT"}, MockedHTTPResponse{StatusCode: 500, Body: "dnm"})
		isFatal, err := mockedAPI.UpdateInstanceWithNewInserts(ctx, instanceID, 1234)
		So(err, ShouldNotBeNil)
		So(isFatal, ShouldBeFalse)
	})

	Convey("When a single import-instance is returned", t, func() {
		mockedAPI := getMockDatasetAPI(http.Request{Method: "PUT"}, MockedHTTPResponse{StatusCode: 200, Body: ""})
		isFatal, err := mockedAPI.UpdateInstanceWithNewInserts(ctx, instanceID, 1234)
		So(err, ShouldBeNil)
		So(isFatal, ShouldBeFalse)
	})
}

func TestUpdateInstanceState(t *testing.T) {
	instanceID := "iid0"
	Convey("When bad request is returned", t, func() {
		mockedAPI := getMockDatasetAPI(http.Request{Method: "PUT"}, MockedHTTPResponse{StatusCode: 400, Body: ""})
		isFatal, err := mockedAPI.UpdateInstanceState(ctx, instanceID, "newState")
		So(err, ShouldNotBeNil)
		So(isFatal, ShouldBeTrue)
	})

	Convey("When server error is returned", t, func() {
		mockedAPI := getMockDatasetAPI(http.Request{Method: "PUT"}, MockedHTTPResponse{StatusCode: 500, Body: "dnm"})
		isFatal, err := mockedAPI.UpdateInstanceState(ctx, instanceID, "newState")
		So(err, ShouldNotBeNil)
		So(isFatal, ShouldBeFalse)
	})

	Convey("When a single import-instance is returned", t, func() {
		mockedAPI := getMockDatasetAPI(http.Request{Method: "PUT"}, MockedHTTPResponse{StatusCode: 200, Body: ""})
		isFatal, err := mockedAPI.UpdateInstanceState(ctx, instanceID, "newState")
		So(err, ShouldBeNil)
		So(isFatal, ShouldBeFalse)
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
