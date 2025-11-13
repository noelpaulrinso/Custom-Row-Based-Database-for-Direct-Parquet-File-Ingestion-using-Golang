package handlers

import (
	"fmt"
	"strings"

	"Custom_DB/pkg/parser"
	"Custom_DB/pkg/schema"
	"Custom_DB/pkg/storage"
)

// HandleDelete processes a DELETE command
func HandleDelete(cmd parser.Command, db *schema.Database) (string, error) {
	tokens := cmd.Tokens
	if len(tokens) < 3 || strings.ToUpper(tokens[1]) != "FROM" {
		return "", fmt.Errorf("invalid DELETE syntax. Example: DELETE FROM table_name WHERE condition;")
	}

	tableName := tokens[2]
	table, exists := db.GetTable(tableName)
	if !exists {
		return "", fmt.Errorf("table '%s' does not exist", tableName)
	}

	// Parse DELETE FROM ... WHERE ...
	fullCommand := strings.Join(tokens, " ")
	whereIndex := strings.Index(strings.ToUpper(fullCommand), " WHERE ")
	
	var wherePart string
	if whereIndex != -1 {
		wherePart = strings.TrimSpace(fullCommand[whereIndex+7:])
	} else {
		return "", fmt.Errorf("DELETE without WHERE clause is not allowed for safety. Use WHERE clause to specify which records to delete")
	}

	// Load table data
	tableFile, err := storage.NewTableFile(db.GetDBPath(), tableName)
	if err != nil {
		return "", fmt.Errorf("error accessing table file: %s", err)
	}

	rows, err := tableFile.ReadAllRows()
	if err != nil {
		return "", fmt.Errorf("error reading table data: %s", err)
	}

	// Filter out rows that match the WHERE clause
	var remainingRows [][]interface{}
	deletedCount := 0

	for _, row := range rows {
		shouldDelete := evaluateWhereClauseDelete(wherePart, row, table.Columns)
		
		if shouldDelete {
			deletedCount++
		} else {
			remainingRows = append(remainingRows, row)
		}
	}

	// Save remaining data
	if err := tableFile.WriteAllRows(remainingRows); err != nil {
		return "", fmt.Errorf("error saving data after deletion: %s", err)
	}

	return fmt.Sprintf("✅ %d row(s) deleted from table '%s'", deletedCount, tableName), nil
}

// Simple WHERE clause evaluation for DELETE
func evaluateWhereClauseDelete(whereClause string, row []interface{}, columns []schema.Column) bool {
	// Simple pattern: column = 'value'
	if strings.Contains(whereClause, "=") {
		parts := strings.Split(whereClause, "=")
		if len(parts) == 2 {
			col := strings.TrimSpace(parts[0])
			val := strings.TrimSpace(parts[1])
			val = strings.Trim(val, "'\"") // Remove quotes

			// Find column index
			for i, column := range columns {
				if strings.EqualFold(column.Name, col) {
					if i < len(row) {
						rowVal := fmt.Sprintf("%v", row[i])
						return strings.EqualFold(rowVal, val)
					}
					break
				}
			}
		}
	}
	return false
}