package gsvt

import (
	"database/sql"
	"fmt"
	"os"
	"testing"
)

func getSqliteDB(t *testing.T) (*sql.DB, func(), error) {
	dbFile := fmt.Sprintf("%s.db", t.Name())
	// If the file already exists, remove it
	if _, err := os.Stat(dbFile); err == nil {
		err = os.Remove(dbFile)
		if err != nil {
			return nil, nil, err
		}
	} else if !os.IsNotExist(err) {
		return nil, nil, err
	}

	db, err := sql.Open("sqlite3", dbFile)
	if err != nil {
		return nil, nil, err
	}

	// Create our cleanup function
	cleanup := func() {
		db.Close()
		// Remove the file
		os.Remove(dbFile)
	}

	return db, cleanup, nil
}
