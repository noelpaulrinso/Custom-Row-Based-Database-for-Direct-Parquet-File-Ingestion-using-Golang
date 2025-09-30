package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"Custom_DB/pkg/handlers"
	"Custom_DB/pkg/parser"
	"Custom_DB/pkg/schema"
	"Custom_DB/pkg/storage"
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
			if err == io.EOF {
				fmt.Println("Exiting CustomDB (EOF).")
				break
			}
			fmt.Println("Error reading input:", err)
			continue
		}
		input = strings.TrimSpace(input)
		input = strings.TrimSuffix(input, ";")

		cmd, perr := parser.Parse(input)
		if perr != nil {
			fmt.Println("Parse error:", perr)
			continue
		}
		command := cmd.Type
		parts := cmd.Tokens

		switch command {
		case "SELECT":
			out, err := handlers.HandleSelect(cmd, db)
			if err != nil {
				fmt.Println("SELECT error:", err)
			} else {
				fmt.Print(out)
			}
			continue

		case "INSERT":
			out, err := handlers.HandleInsert(cmd, db)
			if err != nil {
				fmt.Println("INSERT error:", err)
			} else {
				fmt.Println(out)
			}
			continue

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
			// ... unchanged UPDATE implementation ...
			// (kept as in your original code)
			// ---
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
			// ... unchanged DELETE implementation ...
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
			fmt.Println("Unknown command. Supported commands: CREATE TABLE, INSERT INTO, SELECT, SHOW TABLES, DROP TABLE, UPDATE, DELETE, EXIT.")
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
