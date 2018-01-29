package instance

import "context"

//go:generate moq -out instancetest/store.go -pkg instancetest . Store

type Store interface {
	UpdateInstanceWithHierarchyBuilt(ctx context.Context, instanceID, dimensionID string) (isFatal bool, err error)

	UpdateInstanceWithSearchIndexBuilt(ctx context.Context, instanceID, dimensionID string) (isFatal bool, err error)
}
