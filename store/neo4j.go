package store

import (
	"errors"
	"fmt"

	bolt "github.com/johnnadratowski/golang-neo4j-bolt-driver"
)

const countObservationsStmt = "MATCH (o:`_%s_observation`) RETURN COUNT(o)"

func CountInsertedObservations(dbConn bolt.Conn, instanceID string) (count int64, err error) {
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
