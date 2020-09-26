package pgq

import (
	"context"
	"fmt"
	"testing"

	"github.com/alecthomas/assert"
	"github.com/deciduosity/amboy/job"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

func GetTestDatabase(bctx context.Context, t *testing.T) (*sqlx.DB, func()) {
	ctx, cancel := context.WithCancel(bctx)
	dbName := "amboy_testing_" + uuid.New().String()[0:7]

	tdb, err := sqlx.ConnectContext(ctx, "postgres", "user=amboy database=postgres sslmode=disable")
	require.NoError(t, err)

	_, err = tdb.Exec("CREATE DATABASE " + dbName)
	require.NoError(t, err)

	db, err := sqlx.ConnectContext(ctx, "postgres", fmt.Sprintf("user=amboy database=%s sslmode=disable", dbName))
	require.NoError(t, err)

	closer := func() {
		cancel()

		assert.NoError(t, db.Close())
		_, err := tdb.Exec("DROP DATABASE " + dbName)
		assert.NoError(t, err)
		assert.NoError(t, tdb.Close())
	}
	return db, closer
}

func TestQueue(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	t.Run("RoundTrip", func(t *testing.T) {
		db, close := GetTestDatabase(ctx, t)
		defer close()

		q, err := NewQueue(db, Options{})
		require.NoError(t, err)
		j := job.NewShellJob("ls", "")
		id := j.ID()
		require.NoError(t, q.Put(ctx, j))
		jrt, ok := q.Get(ctx, id)
		require.True(t, ok)
		require.Equal(t, j, jrt)
	})
}
