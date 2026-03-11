package handlers

import (
	"fmt"
	"strconv"
	"strings"

	"Custom_DB/pkg/parser"
	"Custom_DB/pkg/schema"
	"Custom_DB/pkg/storage"
)

// HandleUpdate processes an UPDATE command
func HandleUpdate(cmd parser.Command, db *schema.Database) (string, error) {
	tokens := cmd.Tokens
	if len(tokens) < 4 {
		return "", fmt.Errorf("invalid UPDATE syntax. Example: UPDATE table_name SET column = 'value' WHERE condition;")
	}

	// Parse: UPDATE table_name SET column = 'value' WHERE condition
	tableName := tokens[1] // tokens[0] is "UPDATE", tokens[1] is table name
	table, exists := db.GetTable(tableName)
	if !exists {
		return "", fmt.Errorf("table '%s' does not exist", tableName)
	}

	// Reconstruct full command for easier parsing
	fullCommand := strings.Join(tokens, " ")

	// Find SET and WHERE clauses
	setIndex := strings.Index(strings.ToUpper(fullCommand), " SET ")
	whereIndex := strings.Index(strings.ToUpper(fullCommand), " WHERE ")

	if setIndex == -1 {
		return "", fmt.Errorf("missing SET clause in UPDATE statement")
	}

	var setPart, wherePart string
	if whereIndex != -1 {
		setPart = strings.TrimSpace(fullCommand[setIndex+5 : whereIndex])
		wherePart = strings.TrimSpace(fullCommand[whereIndex+7:])
	} else {
		setPart = strings.TrimSpace(fullCommand[setIndex+5:])
	}

	// Parse SET clause: column = 'value'
	if !strings.Contains(setPart, "=") {
		return "", fmt.Errorf("invalid SET clause syntax. Example: SET column = 'value'")
	}

	setParts := strings.Split(setPart, "=")
	if len(setParts) != 2 {
		return "", fmt.Errorf("invalid SET clause syntax")
	}

	updateColumn := strings.TrimSpace(setParts[0])
	updateValue := strings.TrimSpace(setParts[1])
	updateValue = strings.Trim(updateValue, "'\"") // Remove quotes

	// Validate column exists
	columnExists := false
	for _, col := range table.Columns {
		if strings.EqualFold(col.Name, updateColumn) {
			columnExists = true
			break
		}
	}
	if !columnExists {
		return "", fmt.Errorf("column '%s' does not exist in table '%s'", updateColumn, tableName)
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

	updatedCount := 0

	// Update matching rows
	for i, row := range rows {
		shouldUpdate := true

		// Apply WHERE clause if present
		if wherePart != "" {
			shouldUpdate = evaluateWhereClause(wherePart, row, table.Columns)
		}

		if shouldUpdate {
			rows[i][updateColumn] = updateValue
			updatedCount++
		}
	}

	// Rewrite the entire file with updated data
	if err := tableFile.RewriteFile(rows); err != nil {
		return "", fmt.Errorf("error saving updated data: %s", err)
	}

	return fmt.Sprintf("✅ %d row(s) updated in table '%s'", updatedCount, tableName), nil
}

// Simple WHERE clause evaluation
func evaluateWhereClause(whereClause string, row storage.Row, columns []schema.Column) bool {
	// Simple pattern: column = 'value'
	if strings.Contains(whereClause, "=") {
		parts := strings.Split(whereClause, "=")
		if len(parts) == 2 {
			col := strings.TrimSpace(parts[0])
			val := strings.TrimSpace(parts[1])
			val = strings.Trim(val, "'\"") // Remove quotes

			// Check if row has this column and value matches
			if rowVal, exists := row[col]; exists {
				rowValStr := fmt.Sprintf("%v", rowVal)
				return strings.EqualFold(rowValStr, val)
			}
		}
	}
	return false
}

// Helper function to coerce value to correct type (avoiding duplicate with insert.go)
func coerceUpdateValue(valStr string, targetType schema.DataType) (interface{}, error) {
	trimmedVal := strings.TrimSpace(valStr)

	switch targetType {
	case schema.Integer:
		return strconv.Atoi(trimmedVal)
	case schema.Decimal:
		return strconv.ParseFloat(trimmedVal, 64)
	case schema.Boolean:
		return strconv.ParseBool(trimmedVal)
	case schema.Text:
		return trimmedVal, nil
	case schema.Image:
		return trimmedVal, nil
	default:
		return nil, fmt.Errorf("unsupported data type for coercion: %s", targetType)
	}
}
