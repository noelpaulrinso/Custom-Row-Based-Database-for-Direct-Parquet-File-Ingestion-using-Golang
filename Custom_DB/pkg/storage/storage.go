package storage

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
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

	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory for table file %s: %w", dir, err)
	}

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
	return tf.readAllRowsNoLock()
}

func (tf *TableFile) readAllRowsNoLock() ([]Row, error) {
	file, err := os.OpenFile(tf.path, os.O_RDONLY, 0644)
	if err != nil {
		if os.IsNotExist(err) {
			return []Row{}, nil
		}
		return nil, fmt.Errorf("failed to open file %s for reading: %w", tf.path, err)
	}
	defer file.Close()

	rows := []Row{}
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var row Row
		if err := json.Unmarshal(scanner.Bytes(), &row); err == nil {
			rows = append(rows, row)
		} else {
			fmt.Fprintf(os.Stderr, "Warning: Failed to decode JSON row from %s: %s\n", tf.path, err)
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading file %s: %w", tf.path, err)
	}
	return rows, nil
}

// --- Helper: normalize values for comparison ---
func normalize(val interface{}) interface{} {
	switch v := val.(type) {
	case string:
		// try int
		if i, err := strconv.Atoi(v); err == nil {
			return i
		}
		// try float
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
		// try bool
		if b, err := strconv.ParseBool(v); err == nil {
			return b
		}
		return v
	default:
		return v
	}
}

func (tf *TableFile) UpdateRows(whereCol string, whereVal interface{}, setCol string, setVal interface{}) (int, error) {
	tf.mu.Lock()
	defer tf.mu.Unlock()

	rows, err := tf.readAllRowsNoLock()
	if err != nil {
		return 0, fmt.Errorf("failed to read rows for update: %w", err)
	}

	updatedCount := 0
	whereValNorm := normalize(whereVal)

	for _, row := range rows {
		if val, ok := row[whereCol]; ok && reflect.DeepEqual(normalize(val), whereValNorm) {
			row[setCol] = setVal
			updatedCount++
		}
	}

	if err := tf.rewriteFile(rows); err != nil {
		return 0, fmt.Errorf("failed to rewrite file after update: %w", err)
	}
	return updatedCount, nil
}

func (tf *TableFile) DeleteRows(whereCol string, whereVal interface{}) (int, error) {
	tf.mu.Lock()
	defer tf.mu.Unlock()

	rows, err := tf.readAllRowsNoLock()
	if err != nil {
		return 0, fmt.Errorf("failed to read rows for delete: %w", err)
	}

	deletedCount := 0
	newRows := make([]Row, 0, len(rows))
	whereValNorm := normalize(whereVal)

	for _, row := range rows {
		if val, ok := row[whereCol]; !(ok && reflect.DeepEqual(normalize(val), whereValNorm)) {
			newRows = append(newRows, row)
		} else {
			deletedCount++
		}
	}

	if err := tf.rewriteFile(newRows); err != nil {
		return 0, fmt.Errorf("failed to rewrite file after delete: %w", err)
	}
	return deletedCount, nil
}

func (tf *TableFile) rewriteFile(rows []Row) error {
	tmpPath := tf.path + ".tmp"
	tmpFile, err := os.Create(tmpPath)
	if err != nil {
		return fmt.Errorf("failed to create temporary file for rewrite: %w", err)
	}
	defer tmpFile.Close()
	defer os.Remove(tmpPath)

	for _, row := range rows {
		data, err := json.Marshal(row)
		if err != nil {
			return fmt.Errorf("failed to marshal row to JSON during rewrite: %w", err)
		}
		if _, err := tmpFile.WriteString(string(data) + "\n"); err != nil {
			return fmt.Errorf("failed to write row to temporary file during rewrite: %w", err)
		}
	}

	if err := tmpFile.Close(); err != nil {
		return fmt.Errorf("failed to close temporary file: %w", err)
	}
	if err := os.Rename(tmpPath, tf.path); err != nil {
		return fmt.Errorf("failed to replace original file with temporary file: %w", err)
	}
	return nil
}

func (tf *TableFile) DeleteFile() error {
	tf.mu.Lock()
	defer tf.mu.Unlock()

	if err := os.Remove(tf.path); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("failed to delete table data file %s: %w", tf.path, err)
	}
	return nil
}
