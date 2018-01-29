package event_test

import (
	"context"
	"github.com/ONSdigital/dp-import-tracker/event"
	"github.com/ONSdigital/dp-import-tracker/instance/instancetest"
	"github.com/ONSdigital/dp-import/events"
	"github.com/ONSdigital/dp-reporter-client/reporter/reportertest"
	"github.com/ONSdigital/go-ns/kafka/kafkatest"
	"github.com/pkg/errors"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

var searchIndexBuiltEvent = events.SearchIndexBuilt{
	DimensionName: "dimension",
	InstanceID:    "123",
}

func TestSearchIndexBuiltHandler_Handle(t *testing.T) {

	Convey("Given a handler and a valid event message", t, func() {

		instanceStore := &instancetest.StoreMock{
			UpdateInstanceWithSearchIndexBuiltFunc: func(ctx context.Context, instanceID string, dimensionID string) (bool, error) {
				return false, nil
			},
		}

		errorReporter := reportertest.NewImportErrorReporterMock(nil)
		handler := event.NewSearchIndexBuiltHandler(context.TODO(), instanceStore, errorReporter)

		bytes, _ := events.SearchIndexBuiltSchema.Marshal(searchIndexBuiltEvent)
		message := kafkatest.NewMessage(bytes)

		Convey("When handle is called", func() {

			handler.Handle(message)

			Convey("Then the instance store is called to update the search index build task", func() {
				So(len(instanceStore.UpdateInstanceWithSearchIndexBuiltCalls()), ShouldEqual, 1)
				So(instanceStore.UpdateInstanceWithSearchIndexBuiltCalls()[0].InstanceID, ShouldEqual, searchIndexBuiltEvent.InstanceID)
				So(instanceStore.UpdateInstanceWithSearchIndexBuiltCalls()[0].DimensionID, ShouldEqual, searchIndexBuiltEvent.DimensionName)
			})

			Convey("Then the error reporter is not called", func() {
				So(len(errorReporter.NotifyCalls()), ShouldEqual, 0)
			})

			Convey("Then the message is committed", func() {
				So(message.Committed(), ShouldEqual, true)
			})
		})
	})
}

func TestSearchIndexBuiltHandler_Handle_UnmarshalError(t *testing.T) {

	Convey("Given a handler and an invalid event message", t, func() {

		instanceStore := &instancetest.StoreMock{
			UpdateInstanceWithSearchIndexBuiltFunc: func(ctx context.Context, instanceID string, dimensionID string) (bool, error) {
				return false, nil
			},
		}

		errorReporter := reportertest.NewImportErrorReporterMock(nil)
		handler := event.NewSearchIndexBuiltHandler(context.TODO(), instanceStore, errorReporter)

		message := kafkatest.NewMessage([]byte("invalid message"))

		Convey("When handle is called", func() {

			handler.Handle(message)

			Convey("Then the instance store not called", func() {
				So(len(instanceStore.UpdateInstanceWithSearchIndexBuiltCalls()), ShouldEqual, 0)
			})

			Convey("Then the error reporter is not called", func() {
				So(len(errorReporter.NotifyCalls()), ShouldEqual, 0)
			})

			Convey("Then the message is committed", func() {
				So(message.Committed(), ShouldEqual, true)
			})
		})
	})
}

func TestSearchIndexBuiltHandler_Handle_InstanceStoreError(t *testing.T) {

	Convey("Given a handler with an instanceStore that returns an error", t, func() {

		err := errors.New("instance store is broken")

		instanceStore := &instancetest.StoreMock{
			UpdateInstanceWithSearchIndexBuiltFunc: func(ctx context.Context, instanceID string, dimensionID string) (bool, error) {
				return false, err
			},
		}

		errorReporter := reportertest.NewImportErrorReporterMock(nil)
		handler := event.NewSearchIndexBuiltHandler(context.TODO(), instanceStore, errorReporter)

		bytes, _ := events.SearchIndexBuiltSchema.Marshal(searchIndexBuiltEvent)
		message := kafkatest.NewMessage(bytes)

		Convey("When handle is called", func() {

			handler.Handle(message)

			Convey("Then the instance store is called to update the search index build task", func() {
				So(len(instanceStore.UpdateInstanceWithSearchIndexBuiltCalls()), ShouldEqual, 1)
				So(instanceStore.UpdateInstanceWithSearchIndexBuiltCalls()[0].InstanceID, ShouldEqual, searchIndexBuiltEvent.InstanceID)
				So(instanceStore.UpdateInstanceWithSearchIndexBuiltCalls()[0].DimensionID, ShouldEqual, searchIndexBuiltEvent.DimensionName)
			})

			Convey("Then the error reporter is called", func() {
				So(len(errorReporter.NotifyCalls()), ShouldEqual, 1)
			})

			Convey("Then the message is committed", func() {
				So(message.Committed(), ShouldEqual, true)
			})
		})
	})
}
