package api_test

import (
	"context"
	"net/http"
	"testing"

	importapi "github.com/ONSdigital/dp-api-clients-go/v2/importapi"
	"github.com/ONSdigital/dp-import-tracker/api"
	"github.com/ONSdigital/dp-import-tracker/api/mock"
	. "github.com/smartystreets/goconvey/convey"
)

const (
	jobID = "importJob0"
)

// error definitions for testing
var (
	errImportApiCodeForbidden   = importapi.NewAPIResponse(&http.Response{StatusCode: http.StatusForbidden}, "someUri")
	errImportApiCodeServerError = importapi.NewAPIResponse(&http.Response{StatusCode: http.StatusInternalServerError}, "someUri")
)

func createImportAPIWithMock(clientMock *mock.ImportAPIClientMock) *api.ImportAPI {
	return &api.ImportAPI{
		ServiceAuthToken: token,
		Client:           clientMock,
	}
}

// create mock with GetImportJob implementation
func createGetImportJobMock(importJob importapi.ImportJob, retErr error) *mock.ImportAPIClientMock {
	return &mock.ImportAPIClientMock{
		GetImportJobFunc: func(ctx context.Context, importJobID string, serviceToken string) (importapi.ImportJob, error) {
			return importJob, retErr
		},
	}
}

func createUpdateImportJobStateMock(retErr error) *mock.ImportAPIClientMock {
	return &mock.ImportAPIClientMock{
		UpdateImportJobStateFunc: func(ctx context.Context, jobID string, serviceToken string, newState string) error {
			return retErr
		},
	}
}

func TestGetImportJob(t *testing.T) {

	expectedImportJob := importapi.ImportJob{
		JobID: jobID,
		Links: importapi.LinkMap{
			Instances: []importapi.InstanceLink{
				{
					ID:   "instanceLinkID",
					Link: "instanceLink",
				},
			},
		},
	}

	Convey("When a valid list importJob is returned by GetImportJob then it is returned by the wrapper with no error", t, func() {
		mock := createGetImportJobMock(expectedImportJob, nil)
		importCli := createImportAPIWithMock(mock)
		job, isFatal, err := importCli.GetImportJob(ctx, jobID)
		So(err, ShouldBeNil)
		So(job, ShouldResemble, expectedImportJob)
		So(isFatal, ShouldBeFalse)
		So(len(mock.GetImportJobCalls()), ShouldEqual, 1)
		So(mock.GetImportJobCalls()[0].ImportJobID, ShouldEqual, jobID)
		So(mock.GetImportJobCalls()[0].ServiceToken, ShouldEqual, token)
	})

	Convey("When a generic error is returned by GetImportJob then a fatal error is returned by the wrapper", t, func() {
		mock := createGetImportJobMock(importapi.ImportJob{}, errGeneric)
		importCli := createImportAPIWithMock(mock)
		_, isFatal, err := importCli.GetImportJob(ctx, jobID)
		So(err, ShouldNotBeNil)
		So(isFatal, ShouldBeTrue)
	})

	Convey("When a non-5xx ErrInvalidAPIResponse is returned by PutInstanceImportTasksMock then a fatal error is returned by the wrapper", t, func() {
		mock := createGetImportJobMock(importapi.ImportJob{}, errImportApiCodeForbidden)
		importCli := createImportAPIWithMock(mock)
		_, isFatal, err := importCli.GetImportJob(ctx, jobID)
		So(err, ShouldNotBeNil)
		So(isFatal, ShouldBeTrue)
	})

	Convey("When a 5xx server error ErrInvalidAPIResponse is returned by PutInstanceImportTasksMock then a fatal error is returned by the wrapper", t, func() {
		mock := createGetImportJobMock(importapi.ImportJob{}, errImportApiCodeServerError)
		importCli := createImportAPIWithMock(mock)
		_, isFatal, err := importCli.GetImportJob(ctx, jobID)
		So(err, ShouldNotBeNil)
		So(isFatal, ShouldBeFalse)
	})

}

func TestUpdateImportJobState(t *testing.T) {

	Convey("When a request is made to update import job state and the client returns a generic error", t, func() {
		mock := createUpdateImportJobStateMock(errGeneric)
		importCli := createImportAPIWithMock(mock)
		err := importCli.UpdateImportJobState(ctx, jobID, "completed")

		Convey("Then the response contains the generic error", func() {
			So(err, ShouldResemble, errGeneric)
			So(len(mock.UpdateImportJobStateCalls()), ShouldEqual, 1)
			So(mock.UpdateImportJobStateCalls()[0].JobID, ShouldEqual, jobID)
			So(mock.UpdateImportJobStateCalls()[0].NewState, ShouldEqual, "completed")
			So(mock.UpdateImportJobStateCalls()[0].ServiceToken, ShouldEqual, token)
		})
	})
}
