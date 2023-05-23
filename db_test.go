package gsvt

import (
	"encoding/csv"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"

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
		// Now we have our embedding, we can create
		// our vector
		vector := &Vector{
			Metadata: map[string]interface{}{},
			Vector:   embedding,
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

func TestSimilarity(t *testing.T) {
	vectors, err := getEmbeddings(DB_EMBEDDINGS_FILE)
	require.Nil(t, err)

	inputs, err := getEmbeddings(SEARCH_EMBEDDINGS_FILE)
	require.Nil(t, err)

	// Get our sqlite connection
	sqlite, cleanup, err := getSqliteDB(t)
	require.Nil(t, err)
	defer cleanup()

	// Create our DB
	db := NewDB(sqlite, &Schema{
		Columns: []*Column{},
	}, &VectorConfig{
		Length: 1536,
	})

	// Migrate our db to setup for our
	// test
	err = db.Migrate()
	require.Nil(t, err)

	for _, vector := range vectors {
		err = db.Insert(vector)
		require.Nil(t, err)
	}

	for _, input := range inputs {
		similar, err := db.QuerySimilarity(input, nil, nil)
		require.Nil(t, err)
		require.NotNil(t, similar)
		require.Greater(t, len(similar), 0)

		fmt.Println("=====================================")
		fmt.Println("Input:", input.Metadata["text"])
		for _, vector := range similar {
			fmt.Printf("Similar: %f %\n", 0.0, vector.Metadata["text"])
		}
		fmt.Println("=====================================")
	}
}

func TestQuerySimilarity(t *testing.T) {
	vectors, err := getEmbeddings(DB_EMBEDDINGS_FILE)
	require.Nil(t, err)

	// Get our sqlite connection
	sqlite, cleanup, err := getSqliteDB(t)
	require.Nil(t, err)
	defer cleanup()

	// Create our DB
	db := NewDB(sqlite, &Schema{
		Columns: []*Column{
			{
				Name: "source",
				Type: "TEXT",
			},
			{
				Name: "category",
				Type: "TEXT",
			},
			{
				Name: "created_at",
				Type: "TIMESTAMP",
			},
		},
	}, &VectorConfig{
		Length: 1536,
	})

	// Migrate our db to setup for our
	// test
	err = db.Migrate()
	require.Nil(t, err)

	// Create our sources and categories to
	// search
	source1 := "source1"
	source2 := "source2"
	category1 := "category1"
	category2 := "category2"

	// Insert our vectors
	for _, vector := range vectors {
		if rand.Int()%2 == 0 {
			vector.Metadata["source"] = source2
		} else {
			vector.Metadata["source"] = source1
		}
		if rand.Int()%2 == 0 {
			vector.Metadata["category"] = category2
		} else {
			vector.Metadata["category"] = category1
		}
		vector.Metadata["created_at"] = getRandomTime()

		err = db.Insert(vector)
		require.Nil(t, err)
	}
}
