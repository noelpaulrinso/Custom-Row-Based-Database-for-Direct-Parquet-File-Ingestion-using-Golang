package storage

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
)

type RowID int
type Row map[string]interface{}

type TableFile struct {
	path string
	mu   sync.RWMutex
}

func NewTableFile(dbPath, tableName string) (*TableFile, error) {
	if dbPath == "" || tableName == "" {
		return nil, fmt.Errorf("invalid parameters: dbPath and tableName cannot be empty")
	}

	path := filepath.Join(dbPath, fmt.Sprintf("%s.dat", tableName))

	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory for table file %s: %w", dir, err)
	}

	// Open with read/write, create if not exists
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to create/open data file %s: %w", path, err)
	}
	defer file.Close()

	return &TableFile{
		path: path,
		mu:   sync.RWMutex{},
	}, nil
}

func (tf *TableFile) AppendRow(row Row) error {
	if row == nil {
		return fmt.Errorf("cannot append nil row")
	}

	tf.mu.Lock()
	defer tf.mu.Unlock()

	// Validate row data
	for col, val := range row {
		if val == nil {
			row[col] = "NULL" // Convert nil to "NULL" string
		}
	}

	file, err := os.OpenFile(tf.path, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open file %s for appending: %w", tf.path, err)
	}
	defer file.Close()

	data, err := json.Marshal(row)
	if err != nil {
		return fmt.Errorf("failed to marshal row to JSON: %w", err)
	}

	// Use buffered writer for better performance
	writer := bufio.NewWriter(file)
	if _, err := writer.WriteString(string(data) + "\n"); err != nil {
		return fmt.Errorf("failed to write row to buffer for file %s: %w", tf.path, err)
	}

	if err := writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush row to file %s: %w", tf.path, err)
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

// Enhanced normalization: returns value, and also string representation for fallback comparison
func normalize(val interface{}) (interface{}, string) {
	if val == nil {
		return nil, ""
	}

	switch v := val.(type) {
	case string:
		v = strings.TrimSpace(v)
		// Remove quotes if present
		if len(v) >= 2 && v[0] == '\'' && v[len(v)-1] == '\'' {
			v = v[1 : len(v)-1]
		}
		// try int
		if i, err := strconv.Atoi(v); err == nil {
			return i, v
		}
		// try float
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f, v
		}
		// try bool
		if b, err := strconv.ParseBool(v); err == nil {
			return b, v
		}
		return v, v
	case int:
		return v, fmt.Sprintf("%d", v)
	case float64:
		return v, fmt.Sprintf("%g", v)
	case bool:
		return v, fmt.Sprintf("%t", v)
	default:
		return v, fmt.Sprintf("%v", v)
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
	whereValNorm, whereValStr := normalize(whereVal)

	for _, row := range rows {
		if val, ok := row[whereCol]; ok {
			valNorm, valStr := normalize(val)
			if reflect.DeepEqual(valNorm, whereValNorm) || valStr == whereValStr {
				row[setCol] = setVal
				updatedCount++
			}
		}
	}

	if err := tf.rewriteFile(rows); err != nil {
		return 0, fmt.Errorf("failed to rewrite file after update: %w", err)
	}
	return updatedCount, nil
}

func (tf *TableFile) DeleteRows(whereCol string, whereVal interface{}) (int, error) {
	if whereCol == "" {
		return 0, fmt.Errorf("where column cannot be empty")
	}

	tf.mu.Lock()
	defer tf.mu.Unlock()

	// Read existing rows
	rows, err := tf.readAllRowsNoLock()
	if err != nil {
		return 0, fmt.Errorf("failed to read rows for delete: %w", err)
	}

	deletedCount := 0
	newRows := make([]Row, 0, len(rows))
	whereValNorm, whereValStr := normalize(whereVal)

	// Create temporary file
	tmpPath := tf.path + ".tmp"
	tmpFile, err := os.OpenFile(tmpPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return 0, fmt.Errorf("failed to create temporary file for delete operation: %w", err)
	}
	defer os.Remove(tmpPath) // Clean up in case of failure

	writer := bufio.NewWriter(tmpFile)

	// Process rows and write to temp file
	for _, row := range rows {
		if val, ok := row[whereCol]; ok {
			valNorm, valStr := normalize(val)
			if reflect.DeepEqual(valNorm, whereValNorm) || valStr == whereValStr {
				deletedCount++
				continue
			}
		}
		// Keep this row
		newRows = append(newRows, row)
		data, err := json.Marshal(row)
		if err != nil {
			tmpFile.Close()
			return 0, fmt.Errorf("failed to marshal row during delete: %w", err)
		}
		if _, err := writer.WriteString(string(data) + "\n"); err != nil {
			tmpFile.Close()
			return 0, fmt.Errorf("failed to write row during delete: %w", err)
		}
	}

	// Ensure all data is written
	if err := writer.Flush(); err != nil {
		tmpFile.Close()
		return 0, fmt.Errorf("failed to flush temporary file: %w", err)
	}
	tmpFile.Close()

	// Atomic rename
	if err := os.Rename(tmpPath, tf.path); err != nil {
		return 0, fmt.Errorf("failed to replace original file with new data: %w", err)
	}

	return deletedCount, nil
}

func (tf *TableFile) rewriteFile(rows []Row) error {
	if rows == nil {
		return fmt.Errorf("rows slice cannot be nil")
	}

	tmpPath := tf.path + ".tmp"
	tmpFile, err := os.OpenFile(tmpPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("failed to create temporary file for rewrite: %w", err)
	}
	defer func() {
		tmpFile.Close()
		// Only attempt to remove if the rename wasn't successful
		if _, err := os.Stat(tmpPath); err == nil {
			os.Remove(tmpPath)
		}
	}()

	writer := bufio.NewWriter(tmpFile)

	// Write all rows to the temporary file
	for _, row := range rows {
		// Validate row data
		for col, val := range row {
			if val == nil {
				row[col] = "NULL"
			}
		}

		data, err := json.Marshal(row)
		if err != nil {
			return fmt.Errorf("failed to marshal row to JSON during rewrite: %w", err)
		}
		if _, err := writer.WriteString(string(data) + "\n"); err != nil {
			return fmt.Errorf("failed to write row to temporary file during rewrite: %w", err)
		}
	}

	// Ensure all data is written
	if err := writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush data to temporary file: %w", err)
	}

	// Ensure data is synced to disk
	if err := tmpFile.Sync(); err != nil {
		return fmt.Errorf("failed to sync temporary file to disk: %w", err)
	}

	if err := tmpFile.Close(); err != nil {
		return fmt.Errorf("failed to close temporary file: %w", err)
	}

	// Atomic rename
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
