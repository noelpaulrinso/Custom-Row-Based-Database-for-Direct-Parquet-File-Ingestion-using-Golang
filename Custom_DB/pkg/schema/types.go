package schema

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

type DataType string

const (
	Integer DataType = "INT"
	Text    DataType = "TEXT"
	Decimal DataType = "DECIMAL"
	Boolean DataType = "BOOL"
)

type Column struct {
	Name string   `json:"name"`
	Type DataType `json:"type"`
}

type Table struct {
	Name    string   `json:"name"`
	Columns []Column `json:"columns"`
}

type Database struct {
	Tables         map[string]Table `json:"tables"`
	mu             sync.RWMutex     `json:"-"`
	dbPath         string           `json:"-"`
	schemaFilePath string           `json:"-"`
}

func NewDatabase(dbPath string) (*Database, error) {
	fullPath := filepath.Join(dbPath, "schema.json")
	db := &Database{
		Tables:         make(map[string]Table),
		dbPath:         dbPath,
		schemaFilePath: fullPath,
	}

	if err := os.MkdirAll(dbPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create database directory %s: %w", dbPath, err)
	}

	data, err := os.ReadFile(fullPath)
	if err != nil {
		if os.IsNotExist(err) {
			return db, db.Save()

		}

		return nil, fmt.Errorf("failed to read the schema file %s: %w", fullPath, err)

	}

	if err := json.Unmarshal(data, &db.Tables); err != nil {
		return nil, fmt.Errorf("failed to Unmarshal the data from %s: %w", fullPath, err)
	}

	return db, nil

}

func (db *Database) Save() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	data, err := json.MarshalIndent(db.Tables, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal schema to JSON: %w", err)
	}

	if err := os.WriteFile(db.schemaFilePath, data, 0644); err != nil {
		return fmt.Errorf("failed to write schema file %s: %w", db.schemaFilePath, err)
	}
	return nil
}

func (db *Database) AddTable(table Table) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if _, exists := db.Tables[table.Name]; exists {
		return fmt.Errorf("table '%s' already exists", table.Name)
	}
	db.Tables[table.Name] = table
	return db.Save()
}

func (db *Database) GetTable(name string) (Table, bool) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	table, exists := db.Tables[name]
	return table, exists
}

func (db *Database) RemoveTable(name string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if _, exists := db.Tables[name]; !exists {
		return fmt.Errorf("table '%s' does not exist", name)
	}
	delete(db.Tables, name)
	return db.Save()
}

func (db *Database) GetAllTableNames() []string {
	db.mu.RLock()
	defer db.mu.RUnlock()

	names := make([]string, 0, len(db.Tables))
	for name := range db.Tables {
		names = append(names, name)
	}
	return names
}

func ValidateColumnType(typeStr string) bool {
	switch DataType(typeStr) {
	case Integer, Text, Decimal, Boolean:
		return true
	default:
		return false
	}
}
