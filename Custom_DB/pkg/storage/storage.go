package storage

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

type RowID int

type Row map[string]interface{}

type TableFile struct {
	path string
	mu   sync.RWMutex
}

func NewTableFile(dbPath, tableName string) (*TableFile, error) {

	path := filepath.Join(dbPath, fmt.Sprintf("%s.dat", tableName))

	if _, err := os.Stat(path); os.IsNotExist(err) {
		file, err := os.Create(path)
		if err != nil {
			return nil, fmt.Errorf("failed to create data file %s: %w", path, err)
		}
		file.Close()
	} else if err != nil {
		return nil, fmt.Errorf("error checking file existence %s: %w", path, err)
	}

	return &TableFile{path: path}, nil
}

func (tf *TableFile) AppendRow(row Row) error {
	tf.mu.Lock()
	defer tf.mu.Unlock()

	file, err := os.OpenFile(tf.path, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open file %s for appending: %w", tf.path, err)
	}
	defer file.Close()

	data, err := json.Marshal(row)
	if err != nil {
		return fmt.Errorf("failed to marshal row to JSON: %w", err)
	}

	if _, err := file.WriteString(string(data) + "\n"); err != nil {
		return fmt.Errorf("failed to write row to file %s: %w", tf.path, err)
	}
	return nil
}

func (tf *TableFile) ReadAllRows() ([]Row, error) {
	tf.mu.RLock()
	defer tf.mu.RUnlock()

	file, err := os.OpenFile(tf.path, os.O_RDONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s for reading: %w", tf.path, err)
	}
	defer file.Close()

	rows := []Row{}
	decoder := json.NewDecoder(file)

	for {
		var row Row
		if err := decoder.Decode(&row); err == io.EOF {
			break
		} else if err != nil {
			return nil, fmt.Errorf("failed to decode JSON from file %s: %w", tf.path, err)
		}
		rows = append(rows, row)
	}

	return rows, nil
}
