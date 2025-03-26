package reconnect

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRefreshableDB(t *testing.T) {
	// Add a model to one database.
	require.NoError(t, SetupFirstDB(), "Setup1 failed")

	// Add a model to the other database.
	require.NoError(t, SetupSecondDB(), "Setup2 failed")

	// Setup the RefreshableDB.
	whichOne := 1
	refresher := func(ctx context.Context, config *pgx.ConnConfig) error {
		switch whichOne {
		case 1:
			config.User = "user1"
			config.Password = "password1"
			config.Database = "db1"
			config.Port = 5432
		case 2:
			config.User = "user2"
			config.Password = "password2"
			config.Database = "db2"
			config.Port = 5433
		default:
			return errors.New("unknown database")
		}

		return nil
	}

	db, err := NewRefreshableDB(
		"host=127.0.0.1 sslmode=disable",
		refresher,
	)
	require.NoError(t, err, "failed to create RefreshableDB")

	// Check that we get the expected model from the RefreshableDB.
	var found Model
	require.NoError(t, db.First(&found).Error, "failed to read from db1")
	assert.Equal(t, "first-database", found.Name, "Expected first-database in original db")

	// Change the refresh function behavior so the next connection reads from the
	// other database.
	whichOne = 2

	// Check that we get the expected model from the RefreshableDB.
	require.NoError(t, db.First(&found).Error, "failed to read from db2")
	assert.Equal(t, "second-database", found.Name, "Expected second-database in db2")
}
