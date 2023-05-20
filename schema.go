package gsvt

import (
	"database/sql"
	"fmt"
	"strings"
)

type Schema struct {
	Name    string
	Columns []*Column
	Indexes []*Index
}

type Column struct {
	Name       string
	Type       string
	Required   bool
	Default    string
	PrimaryKey bool
}

type Index struct {
	Name    string
	Columns []*Column
}

// ===========================
// Schema
// ===========================

// FromSQL takes a given sqlite connection and a tablename and
// queries SQLITE to see if the table exists. If it doesn't,
// *Schema will be nil. If it does, it will return a *Schema
// populated as if it was used to generate the table originally
func FromSQL(db *sql.DB, tablename string) (*Schema, error) {
	// Check if table exists
	query := strings.Builder{}
	query.WriteString(`SELECT name FROM sqlite_master WHERE type='table' AND name=?`)

	rows, err := db.Query(query.String(), tablename)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// If we didn't get any rows, the table doesn't exist and we
	// can just safely return nil
	if !rows.Next() {
		return nil, nil
	}

	schema := &Schema{
		Name: tablename,
	}

	columns := []*Column{}
	indexes := []*Index{}

	// Get all columns
	query = strings.Builder{}
	query.WriteString(`PRAGMA table_info("`)
	query.WriteString(tablename)
	query.WriteString(`")`)

	rows, err = db.Query(query.String())
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		column := &Column{}

		var cid string
		var name string
		var colType string
		var notnull int
		var dfltValue sql.NullString
		var pk int

		err = rows.Scan(&cid, &name, &colType, &notnull, &dfltValue, &pk)
		if err != nil {
			return nil, err
		}

		column.Name = name
		column.Type = colType
		column.Required = notnull == 1
		if dfltValue.Valid {
			dflt, err := dfltValue.Value()
			if err != nil {
				return nil, err
			}
			column.Default = fmt.Sprintf("%v", dflt)
		} else {
			column.Default = ""
		}
		column.PrimaryKey = pk == 1

		columns = append(columns, column)
	}

	// Get all indexes
	query = strings.Builder{}
	query.WriteString(`PRAGMA index_list("`)
	query.WriteString(tablename)
	query.WriteString(`")`)

	rows, err = db.Query(query.String())
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		index := &Index{}

		var seq int
		var name string
		var unique int
		var origin string
		var partial int

		err = rows.Scan(
			&seq,
			&name,
			&unique,
			&origin,
			&partial,
		)
		if err != nil {
			return nil, err
		}

		// If the name starts with sqlite_autoindex_, it's an internal
		// index and we can ignore it
		if strings.HasPrefix(name, `sqlite_autoindex_`) {
			continue
		}

		// The index name is prepended by the table/schema name, so we
		// need to remove that
		indexName := strings.TrimPrefix(name, tablename+"_")
		index.Name = indexName

		// Get index columns
		query = strings.Builder{}
		query.WriteString(`PRAGMA index_info(`)
		query.WriteString(name)
		query.WriteString(`)`)

		rowsIndexInfo, err := db.Query(query.String())
		if err != nil {
			return nil, err
		}
		defer rowsIndexInfo.Close()

		for rowsIndexInfo.Next() {
			var seqno int
			var cid int
			var colname string

			err = rowsIndexInfo.Scan(&seqno, &cid, &colname)
			if err != nil {
				return nil, err
			}

			for _, col := range columns {
				if col.Name == colname {
					index.Columns = append(index.Columns, col)
				}
			}
		}

		indexes = append(indexes, index)
	}

	schema.Columns = columns
	schema.Indexes = indexes

	return schema, nil
}

func (s *Schema) Equal(other *Schema) bool {
	// Check if each column is equivalent
	if len(s.Columns) != len(other.Columns) {
		return false
	}

	// Check if each index is equivalent
	if len(s.Indexes) != len(other.Indexes) {
		return false
	}

	// Compare each column
	for _, column := range other.Columns {
		if !column.IsIn(s.Columns) {
			return false
		}
	}

	// Compare each index
	for _, index := range other.Indexes {
		if !index.IsIn(s.Indexes) {
			return false
		}
	}

	// Finally check attributes
	return s.Name == other.Name
}

func (s *Schema) String() string {
	str := fmt.Sprintf(`Schema: %s`, s.Name)

	if len(s.Columns) > 0 {
		str += "\n-Columns:"
		for _, column := range s.Columns {
			str += fmt.Sprintf("\n     -%s", column.String())
		}
	}

	if len(s.Indexes) > 0 {
		str += "\n-Indexes:"
		for _, index := range s.Indexes {
			str += fmt.Sprintf("\n     -%s", index.String())
		}
	}

	return str
}

// CreateTableSQL generates the SQLITE string necessary for a
// CREATE TABLE statement.
func (s *Schema) CreateTableSQL() string {
	result := strings.Builder{}

	result.WriteString(`CREATE TABLE IF NOT EXISTS `)
	result.WriteString(s.Name)
	result.WriteString(`(`)

	for index, col := range s.Columns {
		result.WriteString(col.ColumnSQL())
		if index < len(s.Columns)-1 {
			result.WriteString(`, `)
		}
	}

	result.WriteString(`)`)

	return result.String()
}

// AlterSchemaSQL generates the SQL necessary to alter a table
// to match the other schema. It will rename existing tables
// (this assumes they exist) and DROP existing indexes, then
// Creating the new table, new indexes. Finally, it will
// create a SELECT statement that will copy the data from the
// old table to the new table where applicable (IE same column)
//
// In other words:
//
// base.AlterSchemaSQL(other) results in SQL that takes us FROM
// base TO other.
func (s *Schema) AlterSchemaSQL(other *Schema) []string {
	queries := []string{}
	addColumns, removeColumns, addIndexes, removeIndexes := s.GenerateDifference(other)

	// If we only have indexes to change and no table changes, we just do this
	tableChanges := len(addColumns) != 0 || len(removeColumns) != 0 || s.Name != other.Name

	if tableChanges {
		// Delete old indexes first. We do this as there is no simple way
		// to rename an index, so we'll just expensively rebuild. Likewise,
		// even if we don't change indexes, any table rename will associate
		// exisitng indexes with the old table.
		for _, index := range s.Indexes {
			query := strings.Builder{}
			query.WriteString(`DROP INDEX IF EXISTS `)
			query.WriteString(fmt.Sprintf("%s_%s", s.Name, index.Name))
			queries = append(queries, query.String())
		}

		// Rename our old table name to the temporary name. This is in case
		// the new format has the same name (as in we're just doing column)
		// changes
		tmpTableName := fmt.Sprintf(`%s_tmp`, s.Name)
		query := strings.Builder{}
		query.WriteString(`ALTER TABLE `)
		query.WriteString(s.Name)
		query.WriteString(` RENAME TO `)
		query.WriteString(tmpTableName)
		queries = append(queries, query.String())

		// Create the new table
		queries = append(queries, other.CreateTableSQL())

		// Now create all indexes
		for _, index := range other.Indexes {
			queries = append(queries, index.CreateIndexSQL(other.Name))
		}

		// Find columns that are the same in both schemas
		keptColumns := []*Column{}
		for _, col := range s.Columns {
			if col.IsIn(other.Columns) {
				keptColumns = append(keptColumns, col)
			}
		}
		// Create our migration query
		queries = append(queries, s.SQLMigrate(tmpTableName, keptColumns))
	} else {
		// In this example, we have no table changes, so we can just work
		// with add/remove indexes
		for _, index := range removeIndexes {
			query := strings.Builder{}
			query.WriteString(`DROP INDEX IF EXISTS `)
			query.WriteString(s.Name + "_" + index.Name)
			queries = append(queries, query.String())
		}

		for _, index := range addIndexes {
			queries = append(queries, index.CreateIndexSQL(other.Name))
		}
	}

	return queries
}

// GenerateDifference takes another schema, and proposes the columns
// to add, the columns to remove, the indexes to add, and the indexes
// to remove.
// base.GenerateDifference(other) creates a change that takes
// us FROM base TO other.
func (s *Schema) GenerateDifference(other *Schema) ([]*Column, []*Column, []*Index, []*Index) {
	addColumns := []*Column{}
	removeColumns := []*Column{}
	addIndexes := []*Index{}
	removeIndexes := []*Index{}

	for _, col := range other.Columns {
		if !col.IsIn(s.Columns) {
			addColumns = append(addColumns, col)
		}
	}

	for _, col := range s.Columns {
		if !col.IsIn(other.Columns) {
			removeColumns = append(removeColumns, col)
		}
	}

	for _, index := range other.Indexes {
		if !index.IsIn(s.Indexes) {
			addIndexes = append(addIndexes, index)
		}
	}

	for _, index := range s.Indexes {
		if !index.IsIn(other.Indexes) {
			removeIndexes = append(removeIndexes, index)
		}
	}

	return addColumns, removeColumns, addIndexes, removeIndexes
}

// SqlMigrate will generate SQL for an INSERt statement that will move
// matching rows from the old table to the new table for a given selection
// of columns.
//
// base.Copy(other) generates SQL that takes us FROM base TO other.
func (s *Schema) SQLMigrate(otherTableName string, columns []*Column) string {
	statement := strings.Builder{}

	statement.WriteString(`INSERT INTO `)
	statement.WriteString(s.Name)

	statement.WriteString(`(`)
	for index, col := range columns {
		statement.WriteString(col.Name)
		if index < len(columns)-1 {
			statement.WriteString(`, `)
		}
	}
	statement.WriteString(`)`)

	statement.WriteString(` SELECT `)
	for index, col := range columns {
		statement.WriteString(col.Name)
		if index < len(columns)-1 {
			statement.WriteString(`, `)
		}
	}
	statement.WriteString(` FROM `)
	statement.WriteString(otherTableName)

	return statement.String()
}

// ===========================
// Column
// ===========================

// ColumnSQL makes the SQLITE string necessary for specifying the
// column in the CREATE TABLE statement.
func (c *Column) ColumnSQL() string {
	result := strings.Builder{}

	result.WriteString(c.Name)
	result.WriteString(` `)
	result.WriteString(c.Type)

	if c.Required {
		result.WriteString(` NOT NULL`)
	}

	if c.PrimaryKey {
		result.WriteString(` PRIMARY KEY`)
	}

	if c.Default != "" {
		result.WriteString(` DEFAULT `)
		result.WriteString(c.Default)
	}

	return result.String()
}

func (c *Column) Equal(other *Column) bool {
	return c.Name == other.Name &&
		c.Type == other.Type &&
		c.Required == other.Required &&
		c.Default == other.Default &&
		c.PrimaryKey == other.PrimaryKey
}

func (c *Column) IsIn(columns []*Column) bool {
	for _, column := range columns {
		if column.Equal(c) {
			return true
		}
	}
	return false
}

func (c *Column) String() string {
	return fmt.Sprintf("%s - %s", c.Name, c.Type)
}

// ===========================
// Index
// ===========================

// CreateIndex generates the SQLITE string necessary for a
// CREATE INDEX statement
func (i *Index) CreateIndexSQL(tablename string) string {
	result := strings.Builder{}

	name := fmt.Sprintf(`%s_%s`, tablename, i.Name)

	result.WriteString(`CREATE INDEX IF NOT EXISTS `)
	result.WriteString(name)
	result.WriteString(` ON `)
	result.WriteString(tablename)
	result.WriteString(`(`)

	for index, col := range i.Columns {
		result.WriteString(col.Name)
		if index < len(i.Columns)-1 {
			result.WriteString(`, `)
		}
	}

	result.WriteString(`)`)

	return result.String()
}

func (i *Index) Equal(other *Index) bool {
	// Check if each column is equivalent
	if len(i.Columns) != len(other.Columns) {
		return false
	}

	for _, column := range other.Columns {
		if !column.IsIn(i.Columns) {
			return false
		}
	}

	return i.Name == other.Name
}

func (i *Index) IsIn(indexes []*Index) bool {
	for _, index := range indexes {
		if index.Equal(i) {
			return true
		}
	}
	return false
}

func (i *Index) String() string {
	columnString := "("
	for index, column := range i.Columns {
		if index > 0 {
			columnString += ", "
		}
		columnString += column.Name
	}
	columnString += ")"

	return fmt.Sprintf(`%s - %s`, i.Name, columnString)
}
