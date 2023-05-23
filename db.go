package gsvt

import (
	"database/sql"
	"fmt"
	"sort"

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
	Options  QueryOptions
}

type ColumnFilter struct {
	Column    string
	Operation string
	Value     interface{}
}

type QueryOptions struct {
	Limit int
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
	fmt.Println("EXECUTE CREATE TABLE")
	fmt.Println(query)
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
	placeholderString := fmt.Sprintf("(%s)", placeholders)

	query := fmt.Sprintf(
		`INSERT INTO %s (%s) VALUES (%s)`,
		db.schema.Name,
		columnNames,
		placeholderString,
	)

	// Execute the query
	_, err = db.db.Exec(query, values...)
	return err
}

func (db *DB) validateQueryFilter(filter *Filter) error {
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

		results := make([]interface{}, len(columns))
		err := rows.Scan(results...)
		if err != nil {
			return nil, err
		}

		// Now we iterate through the results and assign
		// their values to the vector
		for index, column := range columns {
			if column == VECTOR_COLUMN_NAME {
				fmt.Println("converting from bytes")
				bytes := results[index].([]byte)
				vector.FromBytes(bytes)
			} else {
				vector.Metadata[column] = results[index]
			}
		}

		vectors = append(vectors, vector)
	}

	return vectors, nil
}

// Query will return a set of vectors that match the
// given filter via its metadata (you can not search)
// on the vector iself).
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
	if filter.Metadata != nil && len(filter.Metadata) != 0 {
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

func (db *DB) QuerySimilarity(target *Vector, filter *Filter, options *SimilarityOptions) ([]*Vector, error) {
	if options == nil {
		options = DEFAULTOPTIONS
	}

	// First we get the vectors that match the filter
	vectors, err := db.Query(filter)
	if err != nil {
		return nil, err
	}

	// Then we calculate the similarity to the target vector
	distances := target.SimilarityToVectorSet(vectors, options)

	// Then we sort the vectors by their distance
	// to the target vector
	sortedVectors := []*Vector{}

	sort.SliceStable(vectors, func(a int, b int) bool {
		return distances[a] < distances[b]
	})

	// If the limit is 0, we can return now
	if filter.Options.Limit == 0 {
		return sortedVectors, nil
	}

	// ...otherwise, return the limit
	return sortedVectors[0:filter.Options.Limit], nil
}
