package handlers

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
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

// HandleInsertWithImages processes an INSERT INTO command with image support
func HandleInsertWithImages(cmd parser.Command, db *schema.Database, imageDir string) (string, error) {
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

		val, err := coerceValueWithImages(valStr, column.Type, imageDir)
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

// Helper function to get column definition
func getColumnDefinition(columns []schema.Column, colName string) (schema.Column, bool) {
	for _, col := range columns {
		if strings.EqualFold(col.Name, colName) {
			return col, true
		}
	}
	return schema.Column{}, false
}

// Coerce value with image support
func coerceValueWithImages(valStr string, targetType schema.DataType, imageDir string) (interface{}, error) {
	trimmedVal := strings.TrimSpace(valStr)

	if targetType == schema.Text && len(trimmedVal) > 1 && trimmedVal[0] == '\'' && trimmedVal[len(trimmedVal)-1] == '\'' {
		trimmedVal = trimmedVal[1 : len(trimmedVal)-1]
	}

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
		// For image type, find the actual image path
		// Remove quotes from the identifier first
		cleanedIdentifier := strings.Trim(trimmedVal, "'\"")
		imagePath := findImagePath(cleanedIdentifier, imageDir)
		if imagePath == "" {
			return "", fmt.Errorf("image file not found for identifier: %s", cleanedIdentifier)
		}
		return imagePath, nil
	default:
		return nil, fmt.Errorf("unsupported data type for coercion: %s", targetType)
	}
}

// Function to find image path based on identifier
func findImagePath(identifier string, imageDir string) string {
	fmt.Printf("DEBUG: Looking for image '%s' in directory '%s'\n", identifier, imageDir)
	
	if imageDir == "" {
		fmt.Println("DEBUG: Image directory is empty")
		return ""
	}
	
	imageExts := []string{".jpg", ".jpeg", ".png", ".gif", ".bmp", ".tiff", ".webp"}
	
	// Try exact match first
	for _, ext := range imageExts {
		fullPath := filepath.Join(imageDir, identifier+ext)
		fmt.Printf("DEBUG: Checking path: %s\n", fullPath)
		if _, err := os.Stat(fullPath); err == nil {
			fmt.Printf("DEBUG: Found exact match: %s\n", fullPath)
			return fullPath
		}
	}
	
	// Try partial match (file containing the identifier)
	files, err := os.ReadDir(imageDir)
	if err != nil {
		fmt.Printf("DEBUG: Error reading directory: %s\n", err)
		return ""
	}
	
	fmt.Printf("DEBUG: Found %d files in directory\n", len(files))
	for _, file := range files {
		fmt.Printf("DEBUG: Checking file: %s\n", file.Name())
		if !file.IsDir() && strings.Contains(file.Name(), identifier) {
			for _, ext := range imageExts {
				if strings.HasSuffix(strings.ToLower(file.Name()), ext) {
					fullPath := filepath.Join(imageDir, file.Name())
					fmt.Printf("DEBUG: Found partial match: %s\n", fullPath)
					return fullPath
				}
			}
		}
	}
	
	fmt.Printf("DEBUG: No image found for identifier: %s\n", identifier)
	return ""
}

// Keep the original coerceValue for backward compatibility
func coerceValue(valStr string, targetType schema.DataType) (interface{}, error) {
	return coerceValueWithImages(valStr, targetType, "")
}
