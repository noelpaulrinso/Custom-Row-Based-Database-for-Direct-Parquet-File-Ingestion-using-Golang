package handlers

import (
	"fmt"
	"strings"

	"Custom_DB/pkg/parser"
	"Custom_DB/pkg/schema"
	"Custom_DB/pkg/storage"
)

// HandleInsert processes an INSERT INTO command
func HandleInsert(cmd parser.Command, db *schema.Database) (string, error) {
	tokens := cmd.Tokens
	if len(tokens) < 4 || strings.ToUpper(tokens[1]) != "INTO" {
		return "", fmt.Errorf("invalid insert syntax. example: INSERT INTO users (id, name) VALUES (1, 'Alice');")
	}

	tableName := tokens[2]
	table, exists := db.GetTable(tableName)
	if !exists {
		return "", fmt.Errorf("table '%s' does not exist", tableName)
	}

	// extract columns + values (naive parser, assumes correct SQL form)
	full := strings.Join(tokens, " ")
	openParenCols := strings.Index(full, "(")
	closeParenCols := strings.Index(full, ")")
	openParenVals := strings.LastIndex(full, "(")
	closeParenVals := strings.LastIndex(full, ")")

	if openParenCols == -1 || closeParenCols == -1 || openParenVals == -1 || closeParenVals == -1 {
		return "", fmt.Errorf("invalid insert syntax: missing column or value list")
	}

	colsStr := full[openParenCols+1 : closeParenCols]
	valsStr := full[openParenVals+1 : closeParenVals]

	cols := strings.Split(colsStr, ",")
	vals := strings.Split(valsStr, ",")

	if len(cols) != len(vals) {
		return "", fmt.Errorf("column count does not match value count")
	}

	row := make(map[string]interface{})
	for i := range cols {
		colName := strings.TrimSpace(cols[i])
		valStr := strings.TrimSpace(vals[i])

		column, found := getColumnDefinition(table.Columns, colName)
		if !found {
			return "", fmt.Errorf("unknown column '%s' in table '%s'", colName, tableName)
		}

		val, err := coerceValue(valStr, column.Type)
		if err != nil {
			return "", fmt.Errorf("error parsing value for column '%s': %s", colName, err)
		}
		row[colName] = val
	}

	tableFile, err := storage.NewTableFile(db.GetDBPath(), tableName)
	if err != nil {
		return "", fmt.Errorf("error opening table file: %s", err)
	}

	if err := tableFile.AppendRow(row); err != nil {
		return "", fmt.Errorf("failed to insert row: %s", err)
	}

	return fmt.Sprintf("1 row inserted into '%s'", tableName), nil
}
