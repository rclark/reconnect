package reconnect

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSwap(t *testing.T) {
	// Add a model to one database.
	db, err := SetupFirstDB()
	require.NoError(t, err, "Setup1 failed")

	// Setup a couple of clients that depend on that database.
	oneClient := Client{db: db}
	anotherClient := Client{db: db}

	// Check that we get the expected model from the client.
	found, err := oneClient.Read()
	require.NoError(t, err, "Use failed on original db")
	assert.Equal(t, "first-database", found.Name, "Expected first-database in original db")

	// Add a model to the other database.
	db2, err := SetupSecondDB()
	require.NoError(t, err, "Setup2 failed")

	// Swap the db pointer, don't really notify the clients in any way.
	*db = *db2

	// Check that both clients now get the model from the other database.
	found, err = oneClient.Read()
	require.NoError(t, err, "Use failed on swapped db")
	assert.Equal(t, "second-database", found.Name, "Expected second-database in swapped db")

	found, err = anotherClient.Read()
	require.NoError(t, err, "Use failed on swapped db")
	assert.Equal(t, "second-database", found.Name, "Expected second-database in swapped db")
}
