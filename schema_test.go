package gsvt

import (
	"database/sql"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	_ "github.com/mattn/go-sqlite3"
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

func TestSchemaCreationAndExtraction(t *testing.T) {
	// Get SQLITE
	db, cleanup, err := getSqliteDB(t)
	require.Nil(t, err)
	require.NotNil(t, db)
	defer cleanup()

	// Build our schema
	columns := getColumns()
	indexes := getIndexes()
	schema := &Schema{
		Name: "TestSchemaPlsIgnore",
		Columns: []*Column{
			columns["id"],
			columns["user"],
			columns["created_at"],
		},
		Indexes: []*Index{
			indexes["id"],
			indexes["user"],
			indexes["created_at"],
			indexes["user_created_at"],
		},
	}

	// First, we ensure that the schema is not in the sqlite database
	foundSchema, err := FromSQL(db, schema.Name)
	require.Nil(t, err)
	assert.Nil(t, foundSchema)

	// Now we create the schema in the SQL database
	query := schema.CreateTableSQL()
	fmt.Println(query)
	_, err = db.Exec(query)
	require.Nil(t, err)

	// Create the indexes as well
	for _, index := range schema.Indexes {
		query := index.CreateIndexSQL(schema.Name)
		fmt.Println(query)
		_, err = db.Exec(query)
		require.Nil(t, err)
	}

	// Now we read that schema back to see if we can confirm the schema
	// is created as we expected
	foundSchema, err = FromSQL(db, schema.Name)
	require.Nil(t, err)
	assert.NotNil(t, foundSchema)
	assert.True(t, schema.Equal(foundSchema))
}

func TestAlterSchemaSQLIndexChanges(t *testing.T) {
	// Get SQLITE
	db, cleanup, err := getSqliteDB(t)
	require.Nil(t, err)
	require.NotNil(t, db)
	defer cleanup()
	baseSchema := getBaseSchema()
	_, err = db.Exec(baseSchema.CreateTableSQL())
	require.Nil(t, err)
	for _, index := range baseSchema.Indexes {
		_, err = db.Exec(index.CreateIndexSQL(baseSchema.Name))
		require.Nil(t, err)
	}
	// Ensure the table was written correctly
	checkSchema, err := FromSQL(db, baseSchema.Name)
	require.Nil(t, err)
	assert.True(t, baseSchema.Equal(checkSchema))

	// Index changes:
	// We'll add one, remove one, and change one.
	newSchema := getBaseSchema()
	// Remove the last ("created_at")
	newSchema.Indexes = newSchema.Indexes[0:2]
	// Add a new index
	columns := getColumns()
	newSchema.Indexes = append(newSchema.Indexes, &Index{
		Name:    "new_index",
		Columns: []*Column{columns["user"]},
	})
	// Change an index (renaming the "id" index)
	newSchema.Indexes[0].Name = "new_name"
	sqlCommands := baseSchema.AlterSchemaSQL(newSchema)
	for _, command := range sqlCommands {
		_, err = db.Exec(command)
		require.Nil(t, err)
	}

	currentSchema, err := FromSQL(db, newSchema.Name)
	require.Nil(t, err)
	assert.True(t, newSchema.Equal(currentSchema))
}

func TestAlterSchemaSQLLargeChanges(t *testing.T) {
	// Get SQLITE
	db, cleanup, err := getSqliteDB(t)
	require.Nil(t, err)
	require.NotNil(t, db)
	defer cleanup()
	baseSchema := getBaseSchema()
	_, err = db.Exec(baseSchema.CreateTableSQL())
	require.Nil(t, err)
	for _, index := range baseSchema.Indexes {
		_, err = db.Exec(index.CreateIndexSQL(baseSchema.Name))
		require.Nil(t, err)
	}
	// Ensure the table was written correctly
	checkSchema, err := FromSQL(db, baseSchema.Name)
	require.Nil(t, err)
	assert.True(t, baseSchema.Equal(checkSchema))

	// Table changes:
	//We'll add a new column and remove the created_at column
	newSchema := getBaseSchema()
	columns := getColumns()
	// Remove the created_at column
	newSchema.Columns = newSchema.Columns[0:2]
	// Add a new column (fake)
	newSchema.Columns = append(newSchema.Columns, columns["fake"])

	// Index changes:
	// We'll remove index and add another
	// Remove the last ("created_at")
	newSchema.Indexes = newSchema.Indexes[0:2]
	// Add a new index
	newSchema.Indexes = append(newSchema.Indexes, &Index{
		Name:    "new_index",
		Columns: []*Column{columns["user"]},
	})
	sqlCommands := baseSchema.AlterSchemaSQL(newSchema)
	for _, command := range sqlCommands {
		_, err = db.Exec(command)
		require.Nil(t, err)
	}

	checkSchema, err = FromSQL(db, newSchema.Name)
	require.Nil(t, err)
	assert.True(t, newSchema.Equal(checkSchema))
}

func TestAlterSchemaSQLTableChangesWithData(t *testing.T) {
	// Get SQLITE
	db, cleanup, err := getSqliteDB(t)
	require.Nil(t, err)
	require.NotNil(t, db)
	defer cleanup()
	baseSchema := getBaseSchema()
	_, err = db.Exec(baseSchema.CreateTableSQL())
	require.Nil(t, err)
	for _, index := range baseSchema.Indexes {
		_, err = db.Exec(index.CreateIndexSQL(baseSchema.Name))
		require.Nil(t, err)
	}

	// Ensure the table was written correctly
	checkSchema, err := FromSQL(db, baseSchema.Name)
	require.Nil(t, err)
	assert.True(t, baseSchema.Equal(checkSchema))

	// Insert data - we're going to generate 10k random
	// pieces of data for our base table, then ensure
	// that it's moved in a way we'd expect.
	datumCount := 1000
	data := map[string]map[string]interface{}{}
	for i := 0; i < datumCount; i++ {
		id := fmt.Sprintf("id_%d", i)
		data[id] = map[string]interface{}{
			"id":         id,
			"user":       i,
			"created_at": time.Now().Add(time.Duration(rand.Int()) * time.Minute),
		}
	}
	for _, data := range data {
		query := fmt.Sprintf("INSERT INTO %s (id, user, created_at) VALUES (?, ?, ?)", baseSchema.Name)
		_, err = db.Exec(query, data["id"], data["user"], data["created_at"])
		require.Nil(t, err)
	}

	// Table changes:
	//We'll add a new column and remove the created_at column
	newSchema := getBaseSchema()
	columns := getColumns()
	// Remove the created_at column
	newSchema.Columns = newSchema.Columns[0:2]
	// Add a new column (fake)
	newSchema.Columns = append(newSchema.Columns, columns["fake"])

	// Index changes:
	// We'll remove index and add another
	// Remove the last ("created_at")
	newSchema.Indexes = newSchema.Indexes[0:2]
	// Add a new index
	newSchema.Indexes = append(newSchema.Indexes, &Index{
		Name:    "new_index",
		Columns: []*Column{columns["user"]},
	})
	sqlCommands := baseSchema.AlterSchemaSQL(newSchema)
	for _, command := range sqlCommands {
		_, err = db.Exec(command)
		require.Nil(t, err)
	}

	checkSchema, err = FromSQL(db, newSchema.Name)
	require.Nil(t, err)
	assert.True(t, newSchema.Equal(checkSchema))

	// Read back our data and ensure it was transferred
	// in an expected manner
	query := fmt.Sprintf("SELECT * FROM %s", newSchema.Name)
	rows, err := db.Query(query)
	require.Nil(t, err)
	defer rows.Close()

	triggered := false
	for rows.Next() {
		triggered = true

		// read back the data
		var id string
		var user int
		var fake sql.NullString

		err = rows.Scan(&id, &user, &fake)
		require.Nil(t, err)

		assert.Equal(t, data[id]["id"], id)
		assert.Equal(t, data[id]["user"], user)
		assert.False(t, fake.Valid)
	}

	// Assure that we actually triggered the loop
	assert.True(t, triggered)
}

func getBaseSchema() *Schema {
	// Build our base schema and write it to the db
	columns := getColumns()
	indexes := getIndexes()
	baseSchema := &Schema{
		Name: "TestSchemaPlsIgnore",
		Columns: []*Column{
			columns["id"],
			columns["user"],
			columns["created_at"],
		},
		Indexes: []*Index{
			indexes["id"],
			indexes["user"],
			indexes["created_at"],
			indexes["user_created_at"],
		},
	}

	return baseSchema
}

func TestSchemaEqual(t *testing.T) {
	// Build two schemas that should be equivalent
	columns := getColumns()
	indexes := getIndexes()

	schema1 := &Schema{
		Name: "SchemaA",
		Columns: []*Column{
			columns["id"],
			columns["user"],
			columns["created_at"],
		},
		Indexes: []*Index{
			indexes["id"],
			indexes["user"],
			indexes["created_at"],
			indexes["user_created_at"],
		},
	}

	// We re-gen the columns and indexes 'cause it's
	// all pointers
	columns = getColumns()
	indexes = getIndexes()

	schema2 := &Schema{
		Name: "SchemaA",
		Columns: []*Column{
			columns["id"],
			columns["user"],
			columns["created_at"],
		},
		Indexes: []*Index{
			indexes["id"],
			indexes["user"],
			indexes["created_at"],
			indexes["user_created_at"],
		},
	}

	// Ensure they're equal
	assert.True(t, schema1.Equal(schema2))

	// Change the name and confirm they're no longer equal
	schema2.Name = "SchemaB"
	assert.False(t, schema1.Equal(schema2))

	// Change the name back, change the value of a column
	// and confirm they're no longer equal
	schema2.Name = "SchemaA"
	assert.True(t, schema1.Equal(schema2))
	schema2.Columns[0].Name = "fake"
	assert.False(t, schema1.Equal(schema2))

	// Change the column name back, confirm that an index
	// change is also detected
	schema2.Columns[0].Name = "id"
	assert.True(t, schema1.Equal(schema2))
	schema2.Indexes[0].Name = "fake"
	assert.False(t, schema1.Equal(schema2))
}

func TestSchemaGenerateDifference(t *testing.T) {
	columns := getColumns()
	indexes := getIndexes()

	baseSchema := &Schema{
		Name: "base",
		Columns: []*Column{
			columns["id"],
			columns["user"],
			columns["created_at"],
		},
		Indexes: []*Index{
			indexes["id"],
			indexes["user"],
			indexes["created_at"],
		},
	}

	otherSchema := &Schema{
		Name: "other",
		Columns: []*Column{
			columns["id"],
			columns["user"],
			columns["fake"],
		},
		Indexes: []*Index{
			indexes["id"],
			indexes["user"],
			indexes["user_created_at"],
		},
	}

	addColumns, removeColumns, addIndexes, removeIndexes := baseSchema.GenerateDifference(otherSchema)

	assert.Equal(t, 1, len(addColumns))
	assert.Equal(t, 1, len(removeColumns))
	assert.Equal(t, 1, len(addIndexes))
	assert.Equal(t, 1, len(removeIndexes))

	assert.True(t, columns["fake"].IsIn(addColumns))
	assert.True(t, columns["created_at"].IsIn(removeColumns))
	assert.True(t, indexes["user_created_at"].IsIn(addIndexes))
	assert.True(t, indexes["created_at"].IsIn(removeIndexes))
}

func TestCreateTable(t *testing.T) {
	columns := getColumns()
	indexes := getIndexes()

	schema := &Schema{
		Name: "test",
		Columns: []*Column{
			columns["id"],
			columns["user"],
			columns["created_at"],
		},
		Indexes: []*Index{
			indexes["id"],
			indexes["user"],
			indexes["created_at"],
			indexes["user_created_at"],
		},
	}

	expected := `CREATE TABLE IF NOT EXISTS test(id TEXT NOT NULL PRIMARY KEY, user INT, created_at TIMESTAMP NOT NULL DEFAULT NOW)`
	assert.Equal(t, expected, schema.CreateTableSQL())

	expectedOutput := []string{
		`CREATE INDEX IF NOT EXISTS test_id ON test(id)`,
		`CREATE INDEX IF NOT EXISTS test_user ON test(user)`,
		`CREATE INDEX IF NOT EXISTS test_created_at ON test(created_at)`,
		`CREATE INDEX IF NOT EXISTS test_user_created_at ON test(user, created_at)`,
	}

	for i, index := range schema.Indexes {
		assert.Equal(t, expectedOutput[i], index.CreateIndexSQL(schema.Name))
	}
}

func TestColumnIsIn(t *testing.T) {
	columnMap := getColumns()

	columns := []*Column{
		columnMap["id"],
		columnMap["user"],
		columnMap["created_at"],
	}

	assert.True(t, columnMap["id"].IsIn(columns))
	assert.True(t, columnMap["user"].IsIn(columns))
	assert.True(t, columnMap["created_at"].IsIn(columns))

	assert.False(t, columnMap["fake"].IsIn(columns))
}

func TestIndexIsIn(t *testing.T) {
	// Generate our indexes first for testing
	indexMap := getIndexes()

	indexes := []*Index{
		indexMap["id"],
		indexMap["user"],
		indexMap["user_created_at"],
	}

	assert.True(t, indexMap["id"].IsIn(indexes))
	assert.True(t, indexMap["user"].IsIn(indexes))
	assert.True(t, indexMap["user_created_at"].IsIn(indexes))

	assert.False(t, indexMap["created_at"].IsIn(indexes))
}

func getColumns() map[string]*Column {
	id := &Column{
		Name:       "id",
		Type:       "TEXT",
		Required:   true,
		Default:    "",
		PrimaryKey: true,
	}
	user := &Column{
		Name:       "user",
		Type:       "INT",
		Required:   false,
		Default:    "",
		PrimaryKey: false,
	}
	createdAt := &Column{
		Name:       "created_at",
		Type:       "TIMESTAMP",
		Required:   true,
		Default:    "NOW",
		PrimaryKey: false,
	}
	ignored := &Column{
		Name:       "fake",
		Type:       "TEXT",
		Required:   false,
		Default:    "",
		PrimaryKey: false,
	}

	return map[string]*Column{
		"id":         id,
		"user":       user,
		"created_at": createdAt,
		"fake":       ignored,
	}
}

func getIndexes() map[string]*Index {
	indexes := map[string]*Index{}
	columns := getColumns()

	indexes["id"] = &Index{
		Name:    "id",
		Columns: []*Column{columns["id"]},
	}

	indexes["user"] = &Index{
		Name:    "user",
		Columns: []*Column{columns["user"]},
	}

	indexes["created_at"] = &Index{
		Name:    "created_at",
		Columns: []*Column{columns["created_at"]},
	}

	indexes["user_created_at"] = &Index{
		Name:    "user_created_at",
		Columns: []*Column{columns["user"], columns["created_at"]},
	}

	return indexes
}
