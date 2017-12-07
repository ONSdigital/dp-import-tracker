package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/ONSdigital/go-ns/log"
	bolt "github.com/johnnadratowski/golang-neo4j-bolt-driver"
)

const countObservationsStmt = "MATCH (o:`_%s_observation`) RETURN COUNT(o)"

type Storable struct {
	pool bolt.ClosableDriverPool
}

type Storer interface {
	Close(context.Context) error
	CountInsertedObservations(string) (int64, error)
}

func New(neoURL string, neoPoolSize int) (storable Storable, err error) {
	pool, err := bolt.NewClosableDriverPool(neoURL, neoPoolSize)
	if err != nil {
		log.ErrorC("could not connect to neo4j", err, nil)
		return
	}

	storable = Storable{
		pool: pool,
	}
	return
}

func (s Storable) Close(ctx context.Context) error {
	err := s.pool.Close()
	return err
}

// CountInsertedObservations check for count of
func (s Storable) CountInsertedObservations(instanceID string) (count int64, err error) {
	dbConn, err := s.pool.OpenPool()
	if err != nil {
		return 0, err
	}
	defer dbConn.Close()

	var rowCursor bolt.Rows
	if rowCursor, err = dbConn.QueryNeo(fmt.Sprintf(countObservationsStmt, instanceID), nil); err != nil {
		return
	}
	defer rowCursor.Close()
	rows, _, err := rowCursor.All()
	if err != nil {
		return
	}
	var ok bool
	if count, ok = rows[0][0].(int64); !ok {
		return -1, errors.New("Did not get result from DB")
	}
	return
}
