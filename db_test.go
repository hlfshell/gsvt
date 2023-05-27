package gsvt

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const DB_EMBEDDINGS_FILE = "test_data/embeddings.csv"
const SEARCH_EMBEDDINGS_FILE = "test_data/embeddings_search.csv"

func getEmbeddings(filepath string) ([]*Vector, error) {
	// Load our csv file of embeddings
	file, err := os.OpenFile(filepath, os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}
	reader := csv.NewReader(file)

	// The CSV is the text input first, then a series of
	// columns that make up our vector
	dataset, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}

	// Create a set of users and sources to assign
	users := []string{"user1", "user2", "user3"}
	sources := []string{"document", "chat", "internet"}

	// Convert the data into Vectors and text
	vectors := []*Vector{}
	text := []string{}
	for _, row := range dataset {
		// First column is the text input.
		// Each column thereafter is a single float32
		// of the embedding
		embedding := []float64{}
		for i, v := range row {
			if i == 0 {
				text = append(text, v)
			} else {
				// Convert the string to float32
				// and append to our vector
				value, err := strconv.ParseFloat(v, 32)
				if err != nil {
					return nil, err
				}
				embedding = append(embedding, value)
			}
		}

		// Choose a random user
		user := users[rand.Intn(len(users))]
		// Choose a random source
		source := sources[rand.Intn(len(sources))]

		// Now we have our embedding, we can create
		// our vector
		vector := &Vector{
			Metadata: map[string]interface{}{
				"text":       text[len(text)-1],
				"created_at": getRandomTime(),
				"source":     source,
				"user":       user,
			},
			Vector: embedding,
		}
		vectors = append(vectors, vector)
	}

	return vectors, nil
}

func getRandomTime() time.Time {
	// Set the minimum and maximum time values
	minTime := time.Date(2023, 5, 01, 0, 0, 0, 0, time.UTC)
	maxTime := time.Date(2023, 6, 01, 0, 0, 0, 0, time.UTC)

	// Calculate the duration between the minimum and maximum time values
	duration := maxTime.Sub(minTime)

	// Generate a random duration within the range of the duration calculated above
	randomDuration := time.Duration(rand.Intn(int(duration))) * time.Second

	// Add the random duration to the minimum time value to get a random time within the specified range
	randomTime := minTime.Add(randomDuration)

	return randomTime
}

func setupVectorsAndDB(sqlite *sql.DB) (*DB, []*Vector, []*Vector, error) {
	vectors, err := getEmbeddings(DB_EMBEDDINGS_FILE)
	if err != nil {
		return nil, nil, nil, err
	}

	inputs, err := getEmbeddings(SEARCH_EMBEDDINGS_FILE)
	if err != nil {
		return nil, nil, nil, err
	}

	// Create our DB
	db := NewDB(sqlite, &Schema{
		Columns: []*Column{
			{
				Name: "text",
				Type: "TEXT",
			},
			{
				Name: "created_at",
				Type: "TIMESTAMP",
			},
			{
				Name: "source",
				Type: "TEXT",
			},
			{
				Name: "user",
				Type: "TEXT",
			},
		},
	}, &VectorConfig{
		Length: 1536,
	})

	// Migrate our db to setup for our
	// test
	err = db.Migrate()
	if err != nil {
		return nil, nil, nil, err
	}

	// Insert our vectors
	for _, vector := range vectors {
		err := db.Insert(vector)
		if err != nil {
			return nil, nil, nil, err
		}
	}

	return db, vectors, inputs, nil
}

// roughQuery is an inefficient test-only method for figuring
// out what a given filter *should* return from the database;
// it's used for determining expected returns. This is possible
// because we're not allowing anything too complicated for
// querying atm.
func roughQuery(db *DB, vectors []*Vector, filter *Filter) []*Vector {
	// If we have no filter, return all vectors
	if filter == nil {
		return vectors
	}

	// If we have a filter, we need to filter our vectors
	// based on the filter
	filtered := []*Vector{}
	for _, vector := range vectors {
		// Go through each column filter and determine
		// if this vector does or does not match.
		// If something is not a match, break
		// to the next vector
		matched := true
		for _, columnFilter := range filter.Metadata {
			// Attempt to take the object to some interface
			// that we can compare on. Yes, this is ugly,
			// but again this is only for test running

			//Find the column we're targeting
			var targetedColumn *Column
			for _, column := range db.schema.Columns {
				if column.Name == columnFilter.Column {
					targetedColumn = column
				}
			}
			if targetedColumn == nil {
				panic("unknown column")
			}

			// Convert the value to the type of the column
			if targetedColumn.Type == "TEXT" {
				value := columnFilter.Value.(string)
				// Now handle all possible operation checks
				switch columnFilter.Operation {
				case "==":
					if vector.Metadata[columnFilter.Column].(string) != value {
						matched = false
						break
					}
				case "!=":
					if vector.Metadata[columnFilter.Column].(string) == value {
						matched = false
						break
					}
				case ">":
					if vector.Metadata[columnFilter.Column].(string) <= value {
						matched = false
						break
					}
				case ">=":
					if vector.Metadata[columnFilter.Column].(string) < value {
						matched = false
						break
					}
				case "<":
					if vector.Metadata[columnFilter.Column].(string) >= value {
						matched = false
						break
					}
				case "<=":
					if vector.Metadata[columnFilter.Column].(string) > value {
						matched = false
						break
					}
				}
			} else if targetedColumn.Type == "TIMESTAMP" {
				var value time.Time
				value = columnFilter.Value.(time.Time)
				// Now handle all possible operation checks
				switch columnFilter.Operation {
				case "==":
					if vector.Metadata[columnFilter.Column].(time.Time) != value {
						matched = false
						break
					}
				case "!=":
					if vector.Metadata[columnFilter.Column].(time.Time) == value {
						matched = false
						break
					}
				case ">":
					if vector.Metadata[columnFilter.Column].(time.Time).Before(value) {
						matched = false
						break
					}
				case ">=":
					if vector.Metadata[columnFilter.Column].(time.Time).Before(value) || vector.Metadata[columnFilter.Column].(time.Time) != value {
						matched = false
						break
					}
				case "<":
					if vector.Metadata[columnFilter.Column].(time.Time).After(value) {
						matched = false
						break
					}
				case "<=":
					if vector.Metadata[columnFilter.Column].(time.Time).After(value) || vector.Metadata[columnFilter.Column].(time.Time) != value {
						matched = false
						break
					}
				}
			} else {
				panic("unsupported column type")
			}
		}

		if matched {
			filtered = append(filtered, vector)
		}
	}

	return filtered
}

func TestSimilarity(t *testing.T) {
	// Get our sqlite connection
	sqlite, cleanup, err := getSqliteDB(t)
	require.Nil(t, err)
	defer cleanup()

	// Setup our db and vectors
	db, vectors, inputs, err := setupVectorsAndDB(sqlite)
	require.Nil(t, err)

	// For each possible search, confirm that
	// we get a set of results; at least one match
	// per search
	defaultFound := 0
	for _, input := range inputs {
		similar, similarities, err := db.QuerySimilarity(input, nil, nil)
		defaultFound += len(similar)
		require.Nil(t, err)
		require.NotNil(t, similar)
		require.Greater(t, len(similar), 0)
		require.NotNil(t, similarities)
		require.Greater(t, len(similarities), 0)
		require.Equal(t, len(similar), len(similarities))
	}

	// Disable std deviation outlier grab; we should get
	// values for all vectors
	for _, input := range inputs {
		similar, similarities, err := db.QuerySimilarity(input, nil, &FilterOptions{
			SimilarityOptions: nil,
			StdDeviations:     0,
			Limit:             0,
		})
		require.Nil(t, err)
		require.NotNil(t, similar)
		require.Greater(t, len(similar), 0)
		require.NotNil(t, similarities)
		require.Greater(t, len(similarities), 0)
		require.Equal(t, len(similar), len(similarities))
		require.Equal(t, len(vectors), len(similar))
	}

	// Now ensure that the limit feature works
	for _, input := range inputs {
		limit := 1
		similar, similarities, err := db.QuerySimilarity(input, nil, &FilterOptions{
			SimilarityOptions: nil,
			StdDeviations:     0,
			Limit:             limit,
		})
		require.Nil(t, err)
		require.NotNil(t, similar)
		require.Greater(t, len(similar), 0)
		require.NotNil(t, similarities)
		require.Greater(t, len(similarities), 0)
		require.Equal(t, len(similar), len(similarities))
		require.Equal(t, limit, len(similar))
	}

	// Ensure that as we lower the standard deviations allowed
	// we get more results
	expandedFound := 0
	for _, input := range inputs {
		similar, similarities, err := db.QuerySimilarity(input, nil, &FilterOptions{
			SimilarityOptions: nil,
			StdDeviations:     0.5,
			Limit:             0,
		})
		expandedFound += len(similar)

		require.Nil(t, err)
		require.NotNil(t, similar)
		require.Greater(t, len(similar), 2)
		require.NotNil(t, similarities)
		require.Greater(t, len(similarities), 2)
		require.Equal(t, len(similar), len(similarities))
	}
	assert.Less(t, defaultFound, expandedFound)
}

func TestQuerySimilarity(t *testing.T) {
	// Get our sqlite connection
	sqlite, cleanup, err := getSqliteDB(t)
	require.Nil(t, err)
	defer cleanup()

	// Setup our db and vectors
	db, vectors, _, err := setupVectorsAndDB(sqlite)
	require.Nil(t, err)

	// First we show that we can recover all vectors
	// with a blank filter
	foundVectors, err := db.Query(nil)
	require.Nil(t, err)
	require.NotNil(t, foundVectors)
	assert.Len(t, foundVectors, len(vectors))

	// Then show that we can recover with a singular filter
	filter := &Filter{
		Metadata: []ColumnFilter{
			{
				Column:    "user",
				Operation: "==",
				Value:     "user3",
			},
			{
				Column:    "source",
				Operation: "==",
				Value:     "document",
			},
		},
	}
	foundVectors, err = db.Query(filter)
	require.Nil(t, err)
	require.NotNil(t, foundVectors)

	comparableVectors := roughQuery(db, vectors, filter)
	assert.Len(t, foundVectors, len(comparableVectors))

	// Try again with different filters
	filter = &Filter{
		Metadata: []ColumnFilter{
			{
				Column:    "user",
				Operation: "==",
				Value:     "user3",
			},
			{
				Column:    "source",
				Operation: "!=",
				Value:     "internet",
			},
		},
	}

	foundVectors, err = db.Query(filter)
	require.Nil(t, err)
	require.NotNil(t, foundVectors)

	comparableVectors = roughQuery(db, vectors, filter)
	assert.Len(t, foundVectors, len(comparableVectors))
}

func TestAlterTableMigration(t *testing.T) {
	// First we create a new db with expected layout
	// Get our sqlite connection
	sqlite, cleanup, err := getSqliteDB(t)
	require.Nil(t, err)
	defer cleanup()

	// Setup our db and vectors
	db, _, _, err := setupVectorsAndDB(sqlite)
	require.Nil(t, err)

	// Now we have a database with a set of vectors in
	// it; let's migrate with a removed and added
	// column by editing our existing schema.
	fmt.Println(db.schema)
	changedSchema := *db.schema
	changedSchema.Columns = append(changedSchema.Columns, &Column{
		Name: "new_column",
		Type: "string",
	})

	changedSchema.Columns = append(changedSchema.Columns[:2], changedSchema.Columns[3:]...)
	fmt.Println("????")
	fmt.Println(db.schema)
	fmt.Println(&changedSchema)
	fmt.Println("????")
	err = db.Migrate()
	require.Nil(t, err)

	// Now read back the schema and ensure that it is
	// what we expect
	schema, err := FromSQL(db.db, db.schema.Name)
	require.Nil(t, err)
	require.NotNil(t, schema)

	assert.True(t, changedSchema.Equal(schema))

	// We then have to ensure that the data is still
	// there, minus our source column

	// Spot check the users (user1, user2, user3)
	// which should match the initial vectors
	// on roughQuery still
}
