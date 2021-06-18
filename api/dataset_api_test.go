package api_test

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"testing"

	dataset "github.com/ONSdigital/dp-api-clients-go/dataset"
	"github.com/ONSdigital/dp-import-tracker/api"
	"github.com/ONSdigital/dp-import-tracker/api/mock"
	. "github.com/smartystreets/goconvey/convey"
)

const (
	instanceID_0 = "iid0"
	instanceID_1 = "iid1"
	dimensionID  = "time"
	token        = "serviceToken"
)

var ctx = context.Background()

// instances structs for testing
var (
	instance0 = dataset.Instance{dataset.Version{
		ID:                   instanceID_0,
		NumberOfObservations: 1122,
	}}
	instance1 = dataset.Instance{dataset.Version{
		ID:                   instanceID_1,
		NumberOfObservations: 13344,
	}}
)

// instances list for testing
var (
	instances = dataset.Instances{
		Items: []dataset.Instance{instance0, instance1},
	}
)

// error definitions for testing
var (
	errDatasetApiCodeForbidden   = dataset.NewDatasetAPIResponse(&http.Response{StatusCode: http.StatusForbidden}, "someUri")
	errDatasetApiCodeServerError = dataset.NewDatasetAPIResponse(&http.Response{StatusCode: http.StatusInternalServerError}, "someUri")
	errGeneric                   = errors.New("someting went wrong in importapi")
)

func createDatasetAPIWithMock(clientMock *mock.DatasetClientMock) *api.DatasetAPI {
	return &api.DatasetAPI{
		ServiceAuthToken: token,
		Client:           clientMock,
		BatchSize:        10,
		MaxWorkers:       20,
	}
}

// create mock with GetInstance implementation
func createGetInstanceMock(getInstanceReturn dataset.Instance, retErr error) *mock.DatasetClientMock {
	return &mock.DatasetClientMock{
		GetInstanceFunc: func(ctx context.Context, userAuthToken string, serviceAuthToken string, collectionID string, instanceID string) (dataset.Instance, error) {
			return getInstanceReturn, retErr
		},
	}
}

// create mock with GetInstances implementation
func createGetInstancesMock(getInstancesReturn dataset.Instances, retErr error) *mock.DatasetClientMock {
	return &mock.DatasetClientMock{
		GetInstancesInBatchesFunc: func(ctx context.Context, userAuthToken string, serviceAuthToken string, collectionID string, vars url.Values, batchSize, maxWorkers int) (dataset.Instances, error) {
			return getInstancesReturn, retErr
		},
	}
}

// create mock with UpdateInstanceWithNewInserts implementation
func createUpdateInstanceWithNewInsertsFunc(retErr error) *mock.DatasetClientMock {
	return &mock.DatasetClientMock{
		UpdateInstanceWithNewInsertsFunc: func(ctx context.Context, serviceAuthToken string, instanceID string, observationsInserted int32) error {
			return retErr
		},
	}
}

// create mock with PutInstanceImportTasks implementation
func createPutInstanceImportTasksMock(retErr error) *mock.DatasetClientMock {
	return &mock.DatasetClientMock{
		PutInstanceImportTasksFunc: func(ctx context.Context, serviceAuthToken string, instanceID string, data dataset.InstanceImportTasks) error {
			return retErr
		},
	}
}

// create mock with PutInstanceState implementation
func createPutInstanceStateMock(retErr error) *mock.DatasetClientMock {
	return &mock.DatasetClientMock{
		PutInstanceStateFunc: func(ctx context.Context, serviceAuthToken string, instanceID string, state dataset.State) error {
			return retErr
		},
	}
}

func TestGetInstance(t *testing.T) {

	instanceID := "iid0"
	Convey("When a valid instance is returned by GetInstance then it is returned by the wrapper with no error", t, func() {
		mock := createGetInstanceMock(instance0, nil)
		datasetCli := createDatasetAPIWithMock(mock)
		instance, isFatal, err := datasetCli.GetInstance(ctx, instanceID)
		So(err, ShouldBeNil)
		So(instance, ShouldResemble, instance0)
		So(isFatal, ShouldBeFalse)
		So(len(mock.GetInstanceCalls()), ShouldEqual, 1)
		So(mock.GetInstanceCalls()[0].InstanceID, ShouldEqual, instanceID_0)
		So(mock.GetInstanceCalls()[0].ServiceAuthToken, ShouldEqual, token)
		So(mock.GetInstanceCalls()[0].UserAuthToken, ShouldEqual, "")
		So(mock.GetInstanceCalls()[0].CollectionID, ShouldEqual, "")
	})

	Convey("When a generic error is returned by GetInstance then a fatal error is returned by the wrapper", t, func() {
		mock := createGetInstanceMock(dataset.Instance{}, errGeneric)
		datasetCli := createDatasetAPIWithMock(mock)
		_, isFatal, err := datasetCli.GetInstance(ctx, instanceID)
		So(err, ShouldNotBeNil)
		So(isFatal, ShouldBeTrue)
	})

	Convey("When a non-5xx ErrInvalidAPIResponse is returned by GetInstance then a fatal error is returned by the wrapper", t, func() {
		mock := createGetInstanceMock(dataset.Instance{}, errDatasetApiCodeForbidden)
		datasetCli := createDatasetAPIWithMock(mock)
		_, isFatal, err := datasetCli.GetInstance(ctx, instanceID)
		So(err, ShouldResemble, errDatasetApiCodeForbidden)
		So(isFatal, ShouldBeTrue)
	})

	Convey("When a 5xx server error ErrInvalidAPIResponse is returned by GetInstance then a fatal error is returned by the wrapper", t, func() {
		mock := createGetInstanceMock(dataset.Instance{}, errDatasetApiCodeServerError)
		datasetCli := createDatasetAPIWithMock(mock)
		_, isFatal, err := datasetCli.GetInstance(ctx, instanceID)
		So(err, ShouldResemble, errDatasetApiCodeServerError)
		So(isFatal, ShouldBeFalse)
	})
}

func TestGetInstances(t *testing.T) {

	Convey("When a valid list of instances is returned by GetInstances then it is returned by the wrapper with no error", t, func() {
		mock := createGetInstancesMock(instances, nil)
		datasetCli := createDatasetAPIWithMock(mock)
		instance, isFatal, err := datasetCli.GetInstances(ctx, url.Values{"key1": []string{"value1"}})
		So(err, ShouldBeNil)
		So(instance, ShouldResemble, instances)
		So(isFatal, ShouldBeFalse)
		So(len(mock.GetInstancesInBatchesCalls()), ShouldEqual, 1)
		So(mock.GetInstancesInBatchesCalls()[0].ServiceAuthToken, ShouldEqual, token)
		So(mock.GetInstancesInBatchesCalls()[0].UserAuthToken, ShouldEqual, "")
		So(mock.GetInstancesInBatchesCalls()[0].CollectionID, ShouldEqual, "")
		So(mock.GetInstancesInBatchesCalls()[0].BatchSize, ShouldEqual, 10)
		So(mock.GetInstancesInBatchesCalls()[0].MaxWorkers, ShouldEqual, 20)
		So(mock.GetInstancesInBatchesCalls()[0].Vars, ShouldResemble, url.Values{"key1": []string{"value1"}})

	})

	Convey("When a generic error is returned by GetInstance then a fatal error is returned by the wrapper", t, func() {
		mock := createGetInstancesMock(instances, errGeneric)
		datasetCli := createDatasetAPIWithMock(mock)
		_, isFatal, err := datasetCli.GetInstances(ctx, url.Values{})
		So(err, ShouldNotBeNil)
		So(isFatal, ShouldBeTrue)
	})

	Convey("When a non-5xx ErrInvalidAPIResponse is returned by GetInstance then a fatal error is returned by the wrapper", t, func() {
		mock := createGetInstancesMock(instances, errDatasetApiCodeForbidden)
		datasetCli := createDatasetAPIWithMock(mock)
		_, isFatal, err := datasetCli.GetInstances(ctx, url.Values{})
		So(err, ShouldResemble, errDatasetApiCodeForbidden)
		So(isFatal, ShouldBeTrue)
	})

	Convey("When a 5xx server error ErrInvalidAPIResponse is returned by GetInstance then a fatal error is returned by the wrapper", t, func() {
		mock := createGetInstancesMock(instances, errDatasetApiCodeServerError)
		datasetCli := createDatasetAPIWithMock(mock)
		_, isFatal, err := datasetCli.GetInstances(ctx, url.Values{})
		So(err, ShouldResemble, errDatasetApiCodeServerError)
		So(isFatal, ShouldBeFalse)
	})
}

func TestSetImportObservationTaskComplete(t *testing.T) {

	expectedImportTask := dataset.InstanceImportTasks{
		ImportObservations: &dataset.ImportObservationsTask{
			State: dataset.StateCompleted.String(),
		},
	}

	Convey("SetImportObservationTaskComplete calls PutInstanceImportTasksMock with the expected parameters", t, func() {
		mock := createPutInstanceImportTasksMock(nil)
		datasetCli := createDatasetAPIWithMock(mock)
		isFatal, err := datasetCli.SetImportObservationTaskComplete(ctx, instanceID_0)
		So(err, ShouldBeNil)
		So(isFatal, ShouldBeFalse)
		So(len(mock.PutInstanceImportTasksCalls()), ShouldEqual, 1)
		So(mock.PutInstanceImportTasksCalls()[0].InstanceID, ShouldEqual, instanceID_0)
		So(mock.PutInstanceImportTasksCalls()[0].ServiceAuthToken, ShouldEqual, token)
		So(mock.PutInstanceImportTasksCalls()[0].Data, ShouldResemble, expectedImportTask)
	})

	Convey("When a generic error is returned by PutInstanceImportTasksMock then a fatal error is returned by SetImportObservationTaskComplete", t, func() {
		mock := createPutInstanceImportTasksMock(errors.New("Generic error"))
		datasetCli := createDatasetAPIWithMock(mock)
		isFatal, err := datasetCli.SetImportObservationTaskComplete(ctx, instanceID_0)
		So(err, ShouldNotBeNil)
		So(isFatal, ShouldBeTrue)
	})

	Convey("When a non-5xx ErrInvalidAPIResponse is returned by PutInstanceImportTasksMock then a fatal error is returned by SetImportObservationTaskComplete", t, func() {
		mock := createPutInstanceImportTasksMock(errDatasetApiCodeForbidden)
		datasetCli := createDatasetAPIWithMock(mock)
		isFatal, err := datasetCli.SetImportObservationTaskComplete(ctx, instanceID_0)
		So(err, ShouldNotBeNil)
		So(isFatal, ShouldBeTrue)
	})

	Convey("When a 5xx server error ErrInvalidAPIResponse is returned by PutInstanceImportTasksMock then a fatal error is returned by SetImportObservationTaskComplete", t, func() {
		mock := createPutInstanceImportTasksMock(errDatasetApiCodeServerError)
		datasetCli := createDatasetAPIWithMock(mock)
		isFatal, err := datasetCli.SetImportObservationTaskComplete(ctx, instanceID_0)
		So(err, ShouldNotBeNil)
		So(isFatal, ShouldBeFalse)
	})
}

func TestUpdateInstanceWithNewInserts(t *testing.T) {

	Convey("UpdateInstanceWithNewInserts calls UpdateInstanceWithNewInserts with the expected parameters", t, func() {
		mock := createUpdateInstanceWithNewInsertsFunc(nil)
		datasetCli := createDatasetAPIWithMock(mock)
		isFatal, err := datasetCli.UpdateInstanceWithNewInserts(ctx, instanceID_0, 555)
		So(err, ShouldBeNil)
		So(isFatal, ShouldBeFalse)
		So(len(mock.UpdateInstanceWithNewInsertsCalls()), ShouldEqual, 1)
		So(mock.UpdateInstanceWithNewInsertsCalls()[0].InstanceID, ShouldEqual, instanceID_0)
		So(mock.UpdateInstanceWithNewInsertsCalls()[0].ServiceAuthToken, ShouldEqual, token)
		So(mock.UpdateInstanceWithNewInsertsCalls()[0].ObservationsInserted, ShouldEqual, 555)
	})

	Convey("When a generic error is returned by PutInstanceImportTasksMock then a fatal error is returned by SetImportObservationTaskComplete", t, func() {
		mock := createUpdateInstanceWithNewInsertsFunc(errGeneric)
		datasetCli := createDatasetAPIWithMock(mock)
		isFatal, err := datasetCli.UpdateInstanceWithNewInserts(ctx, instanceID_0, 555)
		So(err, ShouldNotBeNil)
		So(isFatal, ShouldBeTrue)
	})

	Convey("When a non-5xx ErrInvalidAPIResponse is returned by PutInstanceImportTasksMock then a fatal error is returned by SetImportObservationTaskComplete", t, func() {
		mock := createUpdateInstanceWithNewInsertsFunc(errDatasetApiCodeForbidden)
		datasetCli := createDatasetAPIWithMock(mock)
		isFatal, err := datasetCli.UpdateInstanceWithNewInserts(ctx, instanceID_0, 555)
		So(err, ShouldNotBeNil)
		So(isFatal, ShouldBeTrue)
	})

	Convey("When a 5xx server error ErrInvalidAPIResponse is returned by PutInstanceImportTasksMock then a fatal error is returned by SetImportObservationTaskComplete", t, func() {
		mock := createUpdateInstanceWithNewInsertsFunc(errDatasetApiCodeServerError)
		datasetCli := createDatasetAPIWithMock(mock)
		isFatal, err := datasetCli.UpdateInstanceWithNewInserts(ctx, instanceID_0, 555)
		So(err, ShouldNotBeNil)
		So(isFatal, ShouldBeFalse)
	})
}

func TestUpdateInstanceWithHierarchyBuilt(t *testing.T) {

	expectedImportTask := dataset.InstanceImportTasks{
		BuildHierarchyTasks: []*dataset.BuildHierarchyTask{
			&dataset.BuildHierarchyTask{
				State:         dataset.StateCompleted.String(),
				DimensionName: dimensionID,
			},
		},
	}

	Convey("UpdateInstanceWithHierarchyBuilt calls PutInstanceImportTasksMock with the expected parameters", t, func() {
		mock := createPutInstanceImportTasksMock(nil)
		datasetCli := createDatasetAPIWithMock(mock)
		isFatal, err := datasetCli.UpdateInstanceWithHierarchyBuilt(ctx, instanceID_0, dimensionID)
		So(err, ShouldBeNil)
		So(isFatal, ShouldBeFalse)
		So(len(mock.PutInstanceImportTasksCalls()), ShouldEqual, 1)
		So(mock.PutInstanceImportTasksCalls()[0].InstanceID, ShouldEqual, instanceID_0)
		So(mock.PutInstanceImportTasksCalls()[0].ServiceAuthToken, ShouldEqual, token)
		So(mock.PutInstanceImportTasksCalls()[0].Data, ShouldResemble, expectedImportTask)
	})

	Convey("When a generic error is returned by PutInstanceImportTasksMock then a fatal error is returned by UpdateInstanceWithHierarchyBuilt", t, func() {
		mock := createPutInstanceImportTasksMock(errGeneric)
		datasetCli := createDatasetAPIWithMock(mock)
		isFatal, err := datasetCli.UpdateInstanceWithHierarchyBuilt(ctx, instanceID_0, dimensionID)
		So(err, ShouldNotBeNil)
		So(isFatal, ShouldBeTrue)
	})

	Convey("When a non-5xx ErrInvalidAPIResponse is returned by PutInstanceImportTasksMock then a fatal error is returned by UpdateInstanceWithHierarchyBuilt", t, func() {
		mock := createPutInstanceImportTasksMock(errDatasetApiCodeForbidden)
		datasetCli := createDatasetAPIWithMock(mock)
		isFatal, err := datasetCli.UpdateInstanceWithHierarchyBuilt(ctx, instanceID_0, dimensionID)
		So(err, ShouldNotBeNil)
		So(isFatal, ShouldBeTrue)
	})

	Convey("When a 5xx server error ErrInvalidAPIResponse is returned by PutInstanceImportTasksMock then a fatal error is returned by UpdateInstanceWithHierarchyBuilt", t, func() {
		mock := createPutInstanceImportTasksMock(errDatasetApiCodeServerError)
		datasetCli := createDatasetAPIWithMock(mock)
		isFatal, err := datasetCli.UpdateInstanceWithHierarchyBuilt(ctx, instanceID_0, dimensionID)
		So(err, ShouldNotBeNil)
		So(isFatal, ShouldBeFalse)
	})

}

func TestUpdateInstanceWithSearchIndexBuilt(t *testing.T) {

	expectedImportTask := dataset.InstanceImportTasks{
		BuildSearchIndexTasks: []*dataset.BuildSearchIndexTask{
			&dataset.BuildSearchIndexTask{
				State:         dataset.StateCompleted.String(),
				DimensionName: dimensionID,
			},
		},
	}

	Convey("UpdateInstanceWithSearchIndexBuilt calls PutInstanceImportTasksMock with the expected parameters", t, func() {
		mock := createPutInstanceImportTasksMock(nil)
		datasetCli := createDatasetAPIWithMock(mock)
		isFatal, err := datasetCli.UpdateInstanceWithSearchIndexBuilt(ctx, instanceID_0, dimensionID)
		So(err, ShouldBeNil)
		So(isFatal, ShouldBeFalse)
		So(len(mock.PutInstanceImportTasksCalls()), ShouldEqual, 1)
		So(mock.PutInstanceImportTasksCalls()[0].InstanceID, ShouldEqual, instanceID_0)
		So(mock.PutInstanceImportTasksCalls()[0].ServiceAuthToken, ShouldEqual, token)
		So(mock.PutInstanceImportTasksCalls()[0].Data, ShouldResemble, expectedImportTask)
	})

	Convey("When a generic error is returned by PutInstanceImportTasksMock then a fatal error is returned by UpdateInstanceWithSearchIndexBuilt", t, func() {
		mock := createPutInstanceImportTasksMock(errGeneric)
		datasetCli := createDatasetAPIWithMock(mock)
		isFatal, err := datasetCli.UpdateInstanceWithSearchIndexBuilt(ctx, instanceID_0, dimensionID)
		So(err, ShouldNotBeNil)
		So(isFatal, ShouldBeTrue)
	})

	Convey("When a non-5xx ErrInvalidAPIResponse is returned by PutInstanceImportTasksMock then a fatal error is returned by UpdateInstanceWithSearchIndexBuilt", t, func() {
		mock := createPutInstanceImportTasksMock(errDatasetApiCodeForbidden)
		datasetCli := createDatasetAPIWithMock(mock)
		isFatal, err := datasetCli.UpdateInstanceWithSearchIndexBuilt(ctx, instanceID_0, dimensionID)
		So(err, ShouldNotBeNil)
		So(isFatal, ShouldBeTrue)
	})

	Convey("When a 5xx server error ErrInvalidAPIResponse is returned by PutInstanceImportTasksMock then a fatal error is returned by UpdateInstanceWithSearchIndexBuilt", t, func() {
		mock := createPutInstanceImportTasksMock(errDatasetApiCodeServerError)
		datasetCli := createDatasetAPIWithMock(mock)
		isFatal, err := datasetCli.UpdateInstanceWithSearchIndexBuilt(ctx, instanceID_0, dimensionID)
		So(err, ShouldNotBeNil)
		So(isFatal, ShouldBeFalse)
	})

}

func TestUpdateInstanceState(t *testing.T) {

	Convey("UpdateInstanceState calls PutInstanceImportTasksMock with the expected parameters", t, func() {
		mock := createPutInstanceStateMock(nil)
		datasetCli := createDatasetAPIWithMock(mock)
		isFatal, err := datasetCli.UpdateInstanceState(ctx, instanceID_0, dataset.StateCreated)
		So(err, ShouldBeNil)
		So(isFatal, ShouldBeFalse)
		So(len(mock.PutInstanceStateCalls()), ShouldEqual, 1)
		So(mock.PutInstanceStateCalls()[0].InstanceID, ShouldEqual, instanceID_0)
		So(mock.PutInstanceStateCalls()[0].ServiceAuthToken, ShouldEqual, token)
		So(mock.PutInstanceStateCalls()[0].State, ShouldEqual, dataset.StateCreated)
	})

	Convey("When a generic error is returned by PutInstanceImportTasksMock then a fatal error is returned by UpdateInstanceState", t, func() {
		mock := createPutInstanceStateMock(errGeneric)
		datasetCli := createDatasetAPIWithMock(mock)
		isFatal, err := datasetCli.UpdateInstanceState(ctx, instanceID_0, dataset.StateCreated)
		So(err, ShouldNotBeNil)
		So(isFatal, ShouldBeTrue)
	})

	Convey("When a non-5xx ErrInvalidAPIResponse is returned by PutInstanceImportTasksMock then a fatal error is returned by UpdateInstanceState", t, func() {
		mock := createPutInstanceStateMock(errDatasetApiCodeForbidden)
		datasetCli := createDatasetAPIWithMock(mock)
		isFatal, err := datasetCli.UpdateInstanceState(ctx, instanceID_0, dataset.StateCreated)
		So(err, ShouldNotBeNil)
		So(isFatal, ShouldBeTrue)
	})

	Convey("When a 5xx server error ErrInvalidAPIResponse is returned by PutInstanceImportTasksMock then a fatal error is returned by UpdateInstanceState", t, func() {
		mock := createPutInstanceStateMock(errDatasetApiCodeServerError)
		datasetCli := createDatasetAPIWithMock(mock)
		isFatal, err := datasetCli.UpdateInstanceState(ctx, instanceID_0, dataset.StateCreated)
		So(err, ShouldNotBeNil)
		So(isFatal, ShouldBeFalse)
	})
}
