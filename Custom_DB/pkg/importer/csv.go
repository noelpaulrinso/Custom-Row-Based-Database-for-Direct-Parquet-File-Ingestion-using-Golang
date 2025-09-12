package importer

import (
	"Custom_DB/pkg/schema"
	"Custom_DB/pkg/storage"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

func ImportCSV(filePath string, db *schema.Database) error {
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open csv file: %w", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	headers, err := reader.Read()
	if err != nil {
		return fmt.Errorf("failed to read csv header: %w", err)
	}

	tableName := getTableNameFromPath(filePath)
	var columns []schema.Column
	for _, header := range headers {
		// Simple type inference, defaults to TEXT
		columns = append(columns, schema.Column{Name: header, Type: schema.Text})
	}

	table := schema.Table{
		Name:    tableName,
		Columns: columns,
	}

	if err := db.AddTable(table); err != nil {
		return fmt.Errorf("failed to create table from csv: %w", err)
	}

	tableFile, err := storage.NewTableFile(db.GetDBPath(), tableName)
	if err != nil {
		return fmt.Errorf("failed to create table file: %w", err)
	}

	rowCount := 0
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to read csv record: %w", err)
		}

		row := make(storage.Row)
		for i, value := range record {
			row[headers[i]] = value
		}

		if err := tableFile.AppendRow(row); err != nil {
			return fmt.Errorf("failed to append row: %w", err)
		}
		rowCount++
	}

	fmt.Printf("Successfully imported %d rows into table '%s' from CSV file.\n", rowCount, tableName)
	return nil
}

func getTableNameFromPath(filePath string) string {
	return strings.TrimSuffix(filepath.Base(filePath), filepath.Ext(filePath))
}
