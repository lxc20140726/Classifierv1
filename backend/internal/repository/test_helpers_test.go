package repository

import (
	"database/sql"
	"fmt"
	"sync/atomic"
	"testing"

	dbpkg "github.com/liqiye/classifier/internal/db"
)

var memoryDBCounter uint64

func newTestDB(t *testing.T) *sql.DB {
	t.Helper()

	id := atomic.AddUint64(&memoryDBCounter, 1)
	dsn := fmt.Sprintf("file:classifier_repo_%d?cache=shared&mode=memory", id)

	database, err := dbpkg.Open(dsn)
	if err != nil {
		t.Fatalf("db.Open(%q) error = %v", dsn, err)
	}

	t.Cleanup(func() {
		_ = database.Close()
	})

	return database
}
