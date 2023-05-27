package gsvt

import (
	"database/sql"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/drewlanenga/govector"
)

const VECTOR_COLUMN_NAME = "vector"

type DB struct {
	db     *sql.DB
	schema *Schema
	config *VectorConfig
}

type VectorConfig struct {
	Length int
}

type Filter struct {
	Metadata []ColumnFilter
}

type FilterOptions struct {
	SimilarityOptions *SimilarityOptions

	// StdDeviations is how many standard deviations
	// to accept as a high likelihood match. If this
	// is set to 0 then it will be ignored. By default
	// the value should be ~ 1.5. However, if you have
	// a low number of samples total, it's possible
	// that you will wish to ignore this feature.
	StdDeviations float64

	// Limit is how many vectors max to return
	Limit int
}

var DefaultFilterOptions FilterOptions = FilterOptions{
	SimilarityOptions: DefaultSimilarityOptions,
	StdDeviations:     1.5,
	Limit:             0,
}

type ColumnFilter struct {
	Column    string
	Operation string
	Value     interface{}
}

func NewDB(db *sql.DB, schema *Schema, config *VectorConfig) *DB {
	// If the schema does not have a name set, we need to
	// set one
	if schema.Name == "" {
		schema.Name = "VectorCollection"
	}

	// If the schema does not have a column named "vector",
	// create one of type BLOB
	vectorExists := false
	for _, column := range schema.Columns {
		if column.Name == "vector" {
			vectorExists = true
			break
		}
	}
	if !vectorExists {
		schema.Columns = append(schema.Columns, &Column{
			Name:       "vector",
			Type:       "BLOB",
			Required:   false,
			PrimaryKey: false,
			Default:    "",
		})
	}

	return &DB{
		db:     db,
		schema: schema,
		config: config,
	}
}

// Migrate will take the expected schema, and ensure that the
// table is created or altered to represent the current schema
func (db *DB) Migrate() error {
	// First check to see if we have a current schema for the
	// table.
	discoveredSchema, err := FromSQL(db.db, db.schema.Name)
	if err != nil {
		return err
	}

	if discoveredSchema == nil {
		// We have no existing table, so just create it
		return db.createTable()
	} else {
		return db.alterTable(discoveredSchema)
	}
}

func (db *DB) createTable() error {
	query := db.schema.CreateTableSQL()

	_, err := db.db.Exec(query)
	if err != nil {
		return err
	}
	for _, index := range db.schema.Indexes {
		query := index.CreateIndexSQL(db.schema.Name)
		_, err := db.db.Exec(query)
		if err != nil {
			return err
		}
	}

	return nil
}

func (db *DB) alterTable(other *Schema) error {
	queries := db.schema.AlterSchemaSQL(other)
	for _, query := range queries {
		_, err := db.db.Exec(query)
		if err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) validateInsert(vector *Vector) error {
	// First we ensure that the vector length matches
	// our expected vector length
	if len(vector.Vector) != db.config.Length {
		return fmt.Errorf(
			"vector length %d does not match expected length %d",
			len(vector.Vector),
			db.config.Length,
		)
	}

	// Ensure that that metadata:
	// 1. Has all required columns
	// 2. Does not include a non existent column
	allColumns := map[string]bool{}
	requiredColumns := map[string]bool{}
	for _, column := range db.schema.Columns {
		if column.Required {
			requiredColumns[column.Name] = false
		}
		allColumns[column.Name] = true
	}

	for key, _ := range vector.Metadata {
		//Existence check
		if _, ok := allColumns[key]; !ok {
			return fmt.Errorf("column %s does not exist", key)
		}
		// Required mark
		if _, ok := requiredColumns[key]; ok {
			requiredColumns[key] = true
		}
	}

	// Ensure that all required fields were filled
	for key, value := range requiredColumns {
		if !value {
			return fmt.Errorf("column %s is required", key)
		}
	}

	return nil
}

func (db *DB) Insert(vector *Vector) error {
	err := db.validateInsert(vector)
	if err != nil {
		return err
	}

	// Build our query wth placeholders
	columnNames := ""
	placeholders := ""
	values := []interface{}{}
	for index, column := range db.schema.Columns {
		if index != 0 {
			placeholders += ", "
			columnNames += ", "
		}
		placeholders += "?"
		columnNames += column.Name
		if column.Name == "vector" {
			values = append(values, vector.ToBytes())
		} else {
			values = append(values, vector.Metadata[column.Name])
		}
	}

	query := fmt.Sprintf(
		`INSERT INTO %s (%s) VALUES (%s)`,
		db.schema.Name,
		columnNames,
		placeholders,
	)

	// Execute the query
	_, err = db.db.Exec(query, values...)
	return err
}

func (db *DB) validateQueryFilter(filter *Filter) error {
	// The filter can be nil - this means we're essentially
	// doing a SELECT ALL on our vectors. Less than ideal, but
	// possible and allowed.
	if filter == nil {
		return nil
	}

	// Ensure that the filter only uses columns that exist
	// within the schema
	allColumnNames := map[string]bool{}
	for _, column := range db.schema.Columns {
		allColumnNames[column.Name] = false
	}
	for _, column := range filter.Metadata {
		if _, ok := allColumnNames[column.Column]; !ok {
			return fmt.Errorf("column %s does not exist", column.Column)
		} else if VECTOR_COLUMN_NAME == column.Column {
			return fmt.Errorf("you can not specify %s in your query filter", VECTOR_COLUMN_NAME)
		}
	}

	return nil
}

func (db *DB) rowsToVectors(rows *sql.Rows) ([]*Vector, error) {
	defer rows.Close()

	// Get the columns returned so we can match them
	// with our metadata
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	vectors := []*Vector{}

	for rows.Next() {
		// Build our vector
		vector := &Vector{
			Metadata: map[string]interface{}{},
			Vector:   govector.Vector{},
		}

		values := make([]interface{}, len(columns))
		results := make([]interface{}, len(columns))
		for i := range results {
			results[i] = &values[i]
		}
		err := rows.Scan(results...)
		if err != nil {
			return nil, err
		}

		// Now we iterate through the results and assign
		// their values to the vector
		for index, column := range columns {
			if column == VECTOR_COLUMN_NAME {
				bytes := (values[index]).([]byte)
				// bytes := (*(results[index].(*interface{}))).([]byte)
				vector.FromBytes(bytes)
			} else {
				// Attempt to conver to the correct type
				// for easier use
				// Find the column in the schema
				matched := false
				for _, schemaColumn := range db.schema.Columns {
					if schemaColumn.Name == column {
						matched = true
						switch schemaColumn.Type {
						case "INTEGER":
							vector.Metadata[column] = (values[index]).(int64)
						case "REAL":
							vector.Metadata[column] = (values[index]).(float64)
						case "TEXT":
							vector.Metadata[column] = (values[index]).(string)
						case "BLOB":
							vector.Metadata[column] = (values[index]).([]byte)
						case "TIMESTAMP":
							vector.Metadata[column] = (values[index]).(time.Time)
						}
						break
					}
				}
				// Failsafe if type is unrecognized
				if !matched {
					vector.Metadata[column] = results[index]
				}
			}
		}

		vectors = append(vectors, vector)
	}

	return vectors, nil
}

// Query will return a set of vectors that match the
// given filter via its metadata (you can not search)
// on the vector iself). If the filter is nil then you
// are recalling all vectors - not recommended but
// possible for smaller datasets
func (db *DB) Query(filter *Filter) ([]*Vector, error) {
	if err := db.validateQueryFilter(filter); err != nil {
		return nil, err
	}

	// Build our SELECT clause via the metadata columns
	selectClause := ""
	for index, column := range db.schema.Columns {
		if index > 0 {
			selectClause += ", "
		}
		selectClause += column.Name
	}

	// Build up our query
	var query string
	whereValues := []interface{}{}
	if filter != nil && filter.Metadata != nil && len(filter.Metadata) != 0 {
		// Build our WHERE statement via the filter
		whereClause := ""
		for index, column := range filter.Metadata {
			if index > 0 {
				whereClause += " AND "
			}
			whereClause += fmt.Sprintf(
				"%s %s ?",
				column.Column,
				column.Operation,
			)
			whereValues = append(whereValues, column.Value)
		}

		query = fmt.Sprintf(
			"SELECT %s FROM %s WHERE %s ",
			selectClause,
			db.schema.Name,
			whereClause,
		)
	} else {
		query = fmt.Sprintf(
			"SELECT %s FROM %s ",
			selectClause,
			db.schema.Name,
		)
	}

	// Execute the query and build our search base
	rows, err := db.db.Query(query, whereValues...)
	if err != nil {
		return nil, err
	}

	return db.rowsToVectors(rows)
}

func (db *DB) QuerySimilarity(target *Vector, filter *Filter, options *FilterOptions) ([]*Vector, []float64, error) {
	if options == nil {
		options = &DefaultFilterOptions
	}

	// First we get the vectors that match the filter
	vectors, err := db.Query(filter)
	if err != nil {
		return nil, nil, err
	}

	// Then we calculate the similarity to the target vector
	similarities, err := target.SimilarityToVectorSet(vectors, options.SimilarityOptions)
	if err != nil {
		return nil, nil, err
	}

	// Then we sort the vectors by their distance
	// to the target vector. We need to create a map
	// to track association between similarity scores
	// and the associated vector
	sortMap := map[*Vector]float64{}
	for index, vector := range vectors {
		sortMap[vector] = similarities[index]
	}

	sort.Slice(vectors, func(a int, b int) bool {
		return sortMap[vectors[a]] > sortMap[vectors[b]]
	})

	// Similarly, we want the similarity scores to be
	// sorted; since we have the sortMap, we don't
	// need to worry about sorting again
	sortedSimilarities := make([]float64, len(similarities))
	for index, vector := range vectors {
		sortedSimilarities[index] = sortMap[vector]
	}

	// If the std dev is not 0, we need to find outliers
	// and filter
	if options.StdDeviations > 0 {
		mean, stdDev := meanAndStandardDeviation(sortedSimilarities)
		outlier := mean + (options.StdDeviations * stdDev)

		// Find the index of the first non outlier
		cutoffIndex := 0
		for index, similarity := range sortedSimilarities {
			if similarity < outlier {
				cutoffIndex = index
				break
			}
		}

		// Filter out non-outliers
		vectors = vectors[0:cutoffIndex]
		sortedSimilarities = sortedSimilarities[0:cutoffIndex]
	}

	// If the limit is 0, we can return now
	if options.Limit == 0 {
		return vectors, sortedSimilarities, nil
	} else {
		return vectors[0:options.Limit], sortedSimilarities[0:options.Limit], nil
	}
}

func meanAndStandardDeviation(values []float64) (float64, float64) {
	mean := 0.0
	for _, value := range values {
		mean += value
	}
	mean /= float64(len(values))

	variance := 0.0
	for _, value := range values {
		variance += math.Pow(value-mean, 2)
	}
	variance /= float64(len(values))

	return mean, math.Sqrt(variance)
}
