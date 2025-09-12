package main

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"Custom_DB/pkg/importer"
	"Custom_DB/pkg/schema"
	"Custom_DB/pkg/storage"

	"github.com/ncruces/zenity"
)

func main() {
	fmt.Println("LOADING ###################################################################################################################################")
	fmt.Println("Welcome to CustomDB!")
	fmt.Println("Type 'exit' or 'quit' to leave the shell.")
	fmt.Println("")
	fmt.Println("")
	fmt.Println("")
	fmt.Println("  /$$$$$$                        /$$                                   /$$$$$$$  /$$$$$$$")
	fmt.Println(" /$$__  $$                      | $$                                  | $$__  $$| $$__  $$")
	fmt.Println("| $$       /$$   /$$  /$$$$$$$ /$$$$$$    /$$$$$$  /$$$$$$/$$$$       | $$    $$| $$    $$")
	fmt.Println("| $$      | $$  | $$ /$$_____/|_  $$_/   /$$__  $$| $$_  $$_  $$      | $$  | $$| $$$$$$$")
	fmt.Println("| $$      | $$  | $$|  $$$$$$   | $$    | $$    $$| $$   $$   $$      | $$  | $$| $$__  $$")
	fmt.Println("| $$    $$| $$  | $$  ____  $$  | $$ /$$| $$  | $$| $$ | $$ | $$      | $$  | $$| $$    $$")
	fmt.Println("|  $$$$$$/|  $$$$$$/ /$$$$$$$/  |  $$$$/|  $$$$$$/| $$ | $$ | $$      | $$$$$$$/| $$$$$$$/")
	fmt.Println("   _____/   ______/ |_______/     ___/    ______/ |__/ |__/ |__/      |_______/ |_______/ ")
	fmt.Println("")
	fmt.Println("")
	fmt.Println("")
	fmt.Println("")
	fmt.Println("")
	fmt.Println("")

	dbPath := "data/my_first_db"
	db, err := schema.NewDatabase(dbPath)
	if err != nil {
		fmt.Printf("Failed to initialize database: %s\n", err)
		return
	}

	reader := bufio.NewReader(os.Stdin)

	for {
		fmt.Print("CustomDB> ")
		input, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("Error reading input:", err)
			continue
		}
		input = strings.TrimSpace(input)

		if strings.ToLower(input) == "exit" || strings.ToLower(input) == "quit" {
			fmt.Println("Exiting CustomDB. Goodbye!")
			break
		}

		input = strings.TrimSuffix(input, ";")

		parts := strings.Split(strings.TrimSpace(input), " ")
		if len(parts) == 0 {
			continue
		}
		command := strings.ToUpper(parts[0])

		switch command {
		case "IMPORT":
			filePath, err := zenity.SelectFile(
				zenity.Title("Select a CSV or Parquet file"),
				zenity.FileFilter{
					Name:     "Data files",
					Patterns: []string{"*.csv", "*.parquet"},
				},
			)
			if err != nil {
				if err == zenity.ErrCanceled {
					fmt.Println("Import cancelled.")
				} else {
					fmt.Printf("Error selecting file: %v\n", err)
				}
				continue
			}

			fmt.Printf("Selected file: %s\n", filePath)
			ext := filepath.Ext(filePath)

			var importErr error
			if ext == ".csv" {
				fmt.Println("Calling CSV importer...")
				importErr = importer.ImportCSV(filePath, db)
			} else if ext == ".parquet" {
				fmt.Println("Calling Parquet importer...")
				importErr = importer.ImportParquet(filePath, db)
			} else {
				fmt.Println("Unsupported file type.")
			}

			if importErr != nil {
				fmt.Printf("Error during import: %v\n", importErr)
			}
		case "CREATE":
			if len(parts) < 3 || strings.ToUpper(parts[1]) != "TABLE" {
				fmt.Println("Invalid CREATE TABLE syntax. Example: CREATE TABLE users (id INT, name TEXT);")
				continue
			}

			tableDefIndex := 2
			for i, part := range parts {
				if strings.ToUpper(part) == "TABLE" {
					tableDefIndex = i + 1
					break
				}
			}
			tableDef := strings.Join(parts[tableDefIndex:], " ")

			tableNameEnd := strings.Index(tableDef, "(")
			if tableNameEnd == -1 {
				fmt.Println("Invalid CREATE TABLE syntax. Missing opening parenthesis.")
				continue
			}
			tableName := strings.TrimSpace(tableDef[:tableNameEnd])

			columnsStrEnd := strings.LastIndex(tableDef, ")")
			if columnsStrEnd == -1 {
				fmt.Println("Invalid CREATE TABLE syntax. Missing closing parenthesis.")
				continue
			}
			columnsStr := tableDef[tableNameEnd+1 : columnsStrEnd]

			columnDefs := strings.Split(columnsStr, ",")

			table := schema.Table{
				Name: tableName,
			}
			for _, colDef := range columnDefs {
				colParts := strings.Fields(strings.TrimSpace(colDef))
				if len(colParts) != 2 || !schema.ValidateColumnType(colParts[1]) {
					fmt.Printf("Invalid column definition: '%s'. Expected format 'name TYPE'.\n", colDef)
					continue
				}
				table.Columns = append(table.Columns, schema.Column{Name: colParts[0], Type: schema.DataType(colParts[1])})
			}

			if err := db.AddTable(table); err != nil {
				fmt.Printf("Error creating table: %s\n", err)
			} else {
				fmt.Printf("Table '%s' created successfully.\n", tableName)
			}

		case "INSERT":
			if len(parts) < 3 || strings.ToUpper(parts[1]) != "INTO" {
				fmt.Println("Invalid INSERT INTO syntax. Example: INSERT INTO users VALUES (1, 'Alice');")
				continue
			}

			valuesStartIndex := 0
			for i, part := range parts {
				if strings.ToUpper(part) == "VALUES" {
					valuesStartIndex = i
					break
				}
			}
			if valuesStartIndex == 0 {
				fmt.Println("Invalid INSERT INTO syntax. VALUES clause not found.")
				continue
			}

			tableName := strings.TrimSpace(parts[2])

			table, exists := db.GetTable(tableName)
			if !exists {
				fmt.Printf("Table '%s' does not exist.\n", tableName)
				continue
			}

			valuesStr := strings.Join(parts[valuesStartIndex+1:], " ")
			valuesStart := strings.Index(valuesStr, "(")
			valuesEnd := strings.LastIndex(valuesStr, ")")
			if valuesStart == -1 || valuesEnd == -1 || valuesStart >= valuesEnd {
				fmt.Println("Invalid INSERT INTO syntax. VALUES clause is malformed.")
				continue
			}

			valueList := strings.Split(valuesStr[valuesStart+1:valuesEnd], ",")

			if len(valueList) != len(table.Columns) {
				fmt.Printf("Column count mismatch. Expected %d values, got %d.\n", len(table.Columns), len(valueList))
				continue
			}

			row := make(storage.Row)
			for i, valStr := range valueList {
				colName := table.Columns[i].Name
				trimmedVal := strings.TrimSpace(valStr)
				switch table.Columns[i].Type {
				case schema.Integer:
					if intVal, err := strconv.Atoi(trimmedVal); err == nil {
						row[colName] = intVal
					} else {
						fmt.Printf("Warning: Column '%s' expected INT, got '%s'. Storing as string.\n", colName, trimmedVal)
						row[colName] = trimmedVal
					}
				case schema.Decimal:
					if floatVal, err := strconv.ParseFloat(trimmedVal, 64); err == nil {
						row[colName] = floatVal
					} else {
						fmt.Printf("Warning: Column '%s' expected DECIMAL, got '%s'. Storing as string.\n", colName, trimmedVal)
						row[colName] = trimmedVal
					}
				case schema.Boolean:
					if boolVal, err := strconv.ParseBool(trimmedVal); err == nil {
						row[colName] = boolVal
					} else {
						fmt.Printf("Warning: Column '%s' expected BOOL, got '%s'. Storing as string.\n", colName, trimmedVal)
						row[colName] = trimmedVal
					}
				case schema.Text:
					if len(trimmedVal) > 1 && trimmedVal[0] == '\'' && trimmedVal[len(trimmedVal)-1] == '\'' {
						row[colName] = trimmedVal[1 : len(trimmedVal)-1]
					} else {
						row[colName] = trimmedVal
					}
				default:
					row[colName] = trimmedVal
				}
			}

			tableFile, err := storage.NewTableFile(db.GetDBPath(), tableName)
			if err != nil {
				fmt.Printf("Error getting table file: %s\n", err)
				continue
			}

			if err := tableFile.AppendRow(row); err != nil {
				fmt.Printf("Error inserting row: %s\n", err)
			} else {
				fmt.Println("Row inserted successfully.")
			}

		case "SELECT":
			if len(parts) < 4 || strings.ToUpper(parts[1]) != "*" || strings.ToUpper(parts[2]) != "FROM" {
				fmt.Println("Invalid SELECT syntax. Example: SELECT * FROM users;")
				continue
			}
			tableName := strings.TrimSpace(parts[3])

			table, exists := db.GetTable(tableName)
			if !exists {
				fmt.Printf("Table '%s' does not exist.\n", tableName)
				continue
			}

			tableFile, err := storage.NewTableFile(db.GetDBPath(), tableName)
			if err != nil {
				fmt.Printf("Error getting table file: %s\n", err)
				continue
			}
			rows, err := tableFile.ReadAllRows()
			if err != nil {
				fmt.Printf("Error reading rows: %s\n", err)
				continue
			}

			header := ""
			for _, col := range table.Columns {
				header += fmt.Sprintf("%-20s", col.Name)
			}
			fmt.Println(header)
			fmt.Println(strings.Repeat("-", len(header)))

			for _, row := range rows {
				rowStr := ""
				for _, col := range table.Columns {
					val, ok := row[col.Name]
					if ok {
						rowStr += fmt.Sprintf("%-20v", val)
					} else {
						rowStr += fmt.Sprintf("%-20s", "NULL")
					}
				}
				fmt.Println(rowStr)
			}

		case "SHOW":
			if len(parts) > 1 && strings.ToUpper(parts[1]) == "TABLES" {
				tableNames := db.GetAllTableNames()
				if len(tableNames) == 0 {
					fmt.Println("No tables found.")
				} else {
					fmt.Println("Tables:")
					for _, name := range tableNames {
						fmt.Printf("- %s\n", name)
					}
				}
			} else {
				fmt.Println("Invalid SHOW syntax. Example: SHOW TABLES;")
			}

		case "DROP":
			if len(parts) < 3 || strings.ToUpper(parts[1]) != "TABLE" {
				fmt.Println("Invalid DROP TABLE syntax. Example: DROP TABLE users;")
				continue
			}
			tableName := strings.TrimSpace(parts[2])

			if err := db.RemoveTable(tableName); err != nil {
				fmt.Printf("Error dropping table from schema: %s\n", err)
				continue
			}

			tableFile, err := storage.NewTableFile(db.GetDBPath(), tableName)
			if err != nil {
				fmt.Printf("Warning: Could not get table file for '%s' to delete: %s\n", tableName, err)
			} else {
				if err := tableFile.DeleteFile(); err != nil {
					fmt.Printf("Warning: Could not delete data file for '%s': %s\n", tableName, err)
				}
			}
			fmt.Printf("Table '%s' dropped successfully.\n", tableName)

		case "UPDATE":
			fullCommand := strings.Join(parts, " ")
			setClauseStart := strings.Index(fullCommand, " SET ")
			whereClauseStart := strings.Index(fullCommand, " WHERE ")

			if setClauseStart == -1 || whereClauseStart == -1 || whereClauseStart < setClauseStart {
				fmt.Println("Invalid UPDATE syntax. Example: UPDATE users SET name = 'Bob' WHERE id = 1;")
				continue
			}

			tableName := strings.TrimSpace(fullCommand[len("UPDATE "):setClauseStart])
			table, exists := db.GetTable(tableName)
			if !exists {
				fmt.Printf("Table '%s' does not exist.\n", tableName)
				continue
			}

			setClause := fullCommand[setClauseStart+len(" SET ") : whereClauseStart]
			whereClause := fullCommand[whereClauseStart+len(" WHERE "):]

			setParts := strings.SplitN(setClause, "=", 2)
			if len(setParts) != 2 {
				fmt.Println("Invalid SET clause in UPDATE.")
				continue
			}
			setCol := strings.TrimSpace(setParts[0])
			setValStr := strings.TrimSpace(setParts[1])

			whereParts := strings.SplitN(whereClause, "=", 2)
			if len(whereParts) != 2 {
				fmt.Println("Invalid WHERE clause in UPDATE. Only simple equality supported for now.")
				continue
			}
			whereCol := strings.TrimSpace(whereParts[0])
			whereValStr := strings.TrimSpace(whereParts[1])

			tableFile, err := storage.NewTableFile(db.GetDBPath(), tableName)
			if err != nil {
				fmt.Printf("Error accessing table file: %s\n", err)
				continue
			}

			setColumn, setColumnExists := getColumnDefinition(table.Columns, setCol)
			whereColumn, whereColumnExists := getColumnDefinition(table.Columns, whereCol)

			if !setColumnExists || !whereColumnExists {
				fmt.Printf("Error: Column(s) in SET or WHERE clause not found in table '%s'.\n", tableName)
				continue
			}

			setVal, err := coerceValue(setValStr, setColumn.Type)
			if err != nil {
				fmt.Printf("Error coercing SET value '%s' for column '%s': %s\n", setValStr, setCol, err)
				continue
			}
			whereVal, err := coerceValue(whereValStr, whereColumn.Type)
			if err != nil {
				fmt.Printf("Error coercing WHERE value '%s' for column '%s': %s\n", whereValStr, whereCol, err)
				continue
			}

			updatedCount, err := tableFile.UpdateRows(whereCol, whereVal, setCol, setVal)
			if err != nil {
				fmt.Printf("Error updating rows: %s\n", err)
			} else {
				fmt.Printf("Updated %d row(s) in table '%s'.\n", updatedCount, tableName)
			}

		case "DELETE":
			// Make DELETE command parsing more robust and case-insensitive
			// Accepts: DELETE FROM table WHERE col = value;
			fullCommand := strings.TrimSpace(input)
			upperFull := strings.ToUpper(fullCommand)
			fromIdx := strings.Index(upperFull, " FROM ")
			whereIdx := strings.Index(upperFull, " WHERE ")

			if fromIdx == -1 || whereIdx == -1 || whereIdx < fromIdx {
				fmt.Println("Invalid DELETE syntax. Example: DELETE FROM users WHERE id = 1;")
				continue
			}

			tableName := strings.TrimSpace(fullCommand[fromIdx+len(" FROM ") : whereIdx])
			table, exists := db.GetTable(tableName)
			if !exists {
				fmt.Printf("Table '%s' does not exist.\n", tableName)
				continue
			}

			whereClause := fullCommand[whereIdx+len(" WHERE "):]
			whereParts := strings.SplitN(whereClause, "=", 2)
			if len(whereParts) != 2 {
				fmt.Println("Invalid WHERE clause in DELETE. Only simple equality supported for now.")
				continue
			}
			whereCol := strings.TrimSpace(whereParts[0])
			whereValStr := strings.TrimSpace(whereParts[1])

			tableFile, err := storage.NewTableFile(db.GetDBPath(), tableName)
			if err != nil {
				fmt.Printf("Error accessing table file: %s\n", err)
				continue
			}

			whereColumn, whereColumnExists := getColumnDefinition(table.Columns, whereCol)
			if !whereColumnExists {
				fmt.Printf("Error: Column '%s' in WHERE clause not found in table '%s'.\n", whereCol, tableName)
				continue
			}
			whereVal, err := coerceValue(whereValStr, whereColumn.Type)
			if err != nil {
				fmt.Printf("Error coercing WHERE value '%s' for column '%s': %s\n", whereValStr, whereCol, err)
				continue
			}

			deletedCount, err := tableFile.DeleteRows(whereCol, whereVal)
			if err != nil {
				fmt.Printf("Error deleting rows: %s\n", err)
			} else {
				fmt.Printf("Deleted %d row(s) from table '%s'.\n", deletedCount, tableName)
			}

		default:
			fmt.Println("Unknown command. Supported commands: CREATE TABLE, INSERT INTO, SELECT, SHOW TABLES, DROP TABLE, UPDATE, DELETE, IMPORT, EXIT.")
		}
	}
}

func getColumnDefinition(columns []schema.Column, colName string) (schema.Column, bool) {
	for _, col := range columns {
		if strings.EqualFold(col.Name, colName) {
			return col, true
		}
	}
	return schema.Column{}, false
}

func coerceValue(valStr string, targetType schema.DataType) (interface{}, error) {
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
	default:
		return nil, fmt.Errorf("unsupported data type for coercion: %s", targetType)
	}
}
