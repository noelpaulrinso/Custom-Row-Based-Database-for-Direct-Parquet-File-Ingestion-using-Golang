package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"Custom_DB/pkg/handlers"
	"Custom_DB/pkg/parser"
	"Custom_DB/pkg/schema"
	"Custom_DB/pkg/storage"
)

var imageDirectory string

// Ollama response structure
type OllamaResponse struct {
	Response string `json:"response"`
}

func main() {
	fmt.Println("LOADING ###################################################################################################################################")
	fmt.Println("Welcome to CustomDB with Natural Language Support!")
	fmt.Println("Type SQL commands or natural language queries.")
	fmt.Println("Type 'exit' or 'quit' to leave the shell.")
	fmt.Println("")
	fmt.Println("Examples:")
	fmt.Println("  SQL: SELECT * FROM photos;")
	fmt.Println("  Natural: show me all photos")
	fmt.Println("  Natural: find photo with id 1")
	fmt.Println("  Natural: add new photo called sunset with image beach001")
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

		// Check for exit commands
		if strings.ToUpper(input) == "EXIT" || strings.ToUpper(input) == "QUIT" {
			fmt.Println("Goodbye!")
			break
		}

		// Handle SET IMAGE DIR command
		if strings.HasPrefix(strings.ToUpper(input), "SET IMAGE DIR") {
			handleSetImageDir(input)
			continue
		}

		// Handle SHOW IMAGE DIR command
		if strings.HasPrefix(strings.ToUpper(input), "SHOW IMAGE DIR") {
			if imageDirectory == "" {
				fmt.Println("No image directory set.")
			} else {
				fmt.Printf("Current image directory: %s\n", imageDirectory)
			}
			continue
		}

		// Check if input is natural language (doesn't start with SQL keywords)
		if isNaturalLanguageImproved(input) {
			handleNaturalLanguageQuery(input, db)
			continue
		}

		// Try to parse as SQL
		cmd, perr := parser.Parse(input)
		if perr != nil {
			// If SQL parsing fails, try as natural language
			fmt.Printf("🔍 SQL parse failed, trying natural language interpretation...\n")
			handleNaturalLanguageQuery(input, db)
			continue
		}

		// Execute SQL command
		executeCommand(cmd, db)
	}
}

// Enhanced natural language detection with better pattern recognition
func isNaturalLanguageImproved(input string) bool {
	upperInput := strings.ToUpper(strings.TrimSpace(input))
	
	// Definite SQL keywords that indicate structured SQL
	sqlKeywords := []string{"SELECT ", "INSERT ", "UPDATE ", "DELETE ", "CREATE ", "DROP "}
	for _, keyword := range sqlKeywords {
		if strings.HasPrefix(upperInput, keyword) {
			return false
		}
	}
	
	// Special case for SHOW command - check if it's proper SQL syntax
	if strings.HasPrefix(upperInput, "SHOW ") {
		// "SHOW TABLES;" is SQL, but "SHOW ME ALL TABLES" is natural language
		if strings.TrimSpace(upperInput) == "SHOW TABLES" || strings.TrimSpace(upperInput) == "SHOW TABLES;" {
			return false
		}
		// Other SHOW variations are likely natural language
		return true
	}
	
	// Strong natural language indicators
	nlPatterns := []string{
		"SHOW ME", "TELL ME", "GET ME", "FIND", "LIST", "WHAT", "WHERE", "HOW MANY", 
		"WHICH", "WHO", "CAN YOU", "PLEASE", "I WANT", "I NEED", "GIVE ME",
		"ALL THE", "DO I HAVE", "ARE THERE", "IS THERE",
	}
	
	for _, pattern := range nlPatterns {
		if strings.Contains(upperInput, pattern) {
			return true
		}
	}
	
	// Question patterns
	if strings.HasSuffix(upperInput, "?") {
		return true
	}
	
	// Table/database related conversational queries
	if (strings.Contains(upperInput, "TABLE") || strings.Contains(upperInput, "DATABASE")) &&
	   !strings.Contains(upperInput, " FROM ") &&
	   !strings.Contains(upperInput, " INTO ") &&
	   !strings.Contains(upperInput, " SET ") {
		return true
	}
	
	// If it contains conversational words without SQL structure
	conversationalWords := []string{"MY", "ALL", "EVERY", "ANY", "SOME"}
	for _, word := range conversationalWords {
		if strings.Contains(upperInput, word) && 
		   !strings.Contains(upperInput, " FROM ") &&
		   !strings.Contains(upperInput, " WHERE ") {
			return true
		}
	}
	
	// Default to natural language for ambiguous cases
	return true
}

// Handle natural language queries with improved processing
func handleNaturalLanguageQuery(input string, db *schema.Database) {
	fmt.Printf("🤖 Processing natural language: \"%s\"\n", input)
	
	// Quick pattern matching for common queries (fallback when Ollama is not available)
	upperInput := strings.ToUpper(input)
	
	// Direct handling for table listing queries
	if strings.Contains(upperInput, "TABLES") || 
	   strings.Contains(upperInput, "WHAT TABLES") ||
	   strings.Contains(upperInput, "WHICH TABLES") ||
	   strings.Contains(upperInput, "SHOW TABLES") ||
	   strings.Contains(upperInput, "LIST TABLES") ||
	   strings.Contains(upperInput, "ALL TABLES") ||
	   (strings.Contains(upperInput, "WHAT") && strings.Contains(upperInput, "HAVE")) ||
	   (strings.Contains(upperInput, "WHICH") && (strings.Contains(upperInput, "DATABASE") || strings.Contains(upperInput, "TABLES"))) {
		fmt.Printf("📝 Generated SQL: SHOW TABLES;\n")
		fmt.Print("Execute this query? (y/n): ")
		reader := bufio.NewReader(os.Stdin)
		confirmation, _ := reader.ReadString('\n')
		confirmation = strings.TrimSpace(strings.ToLower(confirmation))
		
		if confirmation == "y" || confirmation == "yes" {
			executeGeneratedSQL("SHOW TABLES;", db)
		} else {
			fmt.Println("Query cancelled.")
		}
		return
	}
	
	// Build enhanced context about database
	context := buildDatabaseContext(db)
	
	// Create enhanced prompt for Llama3
	prompt := fmt.Sprintf(`You are a SQL expert for a custom database system. Convert natural language to SQL.

%s

IMPORTANT RULES:
1. For "show tables", "list tables", "what tables", "which tables" queries → Use: SHOW TABLES;
2. For counting records → Use: SELECT COUNT(*) FROM table_name;
3. For text comparisons, be case-sensitive or use exact case matching
4. For UPDATE operations, use: UPDATE table_name SET column = 'value' WHERE condition;
5. For DELETE operations, use: DELETE FROM table_name WHERE condition;
6. For DROP TABLE operations, use: DROP TABLE table_name;
7. Return ONLY the SQL query, nothing else
8. End with semicolon
9. Use exact table and column names from the schema above

EXAMPLES:
- "show me all tables" → SHOW TABLES;
- "what tables do I have" → SHOW TABLES;
- "list all tables" → SHOW TABLES;
- "which tables exist" → SHOW TABLES;
- "show all students" → SELECT * FROM students;
- "how many students" → SELECT COUNT(*) FROM students;
- "find student named John" → SELECT * FROM students WHERE name = 'John';
- "what course is Abel taking" → SELECT course FROM students WHERE name = 'Abel';
- "students in AI course" → SELECT * FROM students WHERE course = 'AI/ML';

UPDATE EXAMPLES:
- "change student course to CS where name is John" → UPDATE students SET course = 'CS' WHERE name = 'John';
- "update Abel's course to AI" → UPDATE students SET course = 'AI' WHERE name = 'Abel';
- "set course to Data Science for student with id 3" → UPDATE students SET course = 'Data Science' WHERE id = 3;
- "modify student name to Mike where id is 2" → UPDATE students SET name = 'Mike' WHERE id = 2;
- "change course to EE for student named Dan" → UPDATE students SET course = 'EE' WHERE name = 'Dan';

DELETE EXAMPLES:
- "delete student where name is John" → DELETE FROM students WHERE name = 'John';
- "remove student with id 5" → DELETE FROM students WHERE id = 5;
- "delete all records from students where course is Mech" → DELETE FROM students WHERE course = 'Mech';
- "remove students where course is CS" → DELETE FROM students WHERE course = 'CS';

DROP TABLE EXAMPLES:
- "drop table students" → DROP TABLE students;
- "delete table photos" → DROP TABLE photos;
- "remove the users table" → DROP TABLE users;
- "drop the teacher table" → DROP TABLE teacher;

Natural Language: %s

SQL:`, context, input)

	// Query Ollama with improved parameters
	sqlQuery, err := queryOllamaImproved(prompt)
	if err != nil {
		fmt.Printf("❌ Error connecting to Llama3: %s\n", err)
		fmt.Println("💡 Make sure Ollama is running: 'ollama serve'")
		fmt.Println("💡 And Llama3 is installed: 'ollama pull llama3'")
		fmt.Println("💡 Using basic pattern matching as fallback...")
		
		// Basic fallback for common patterns
		handleBasicPatternMatching(input, db)
		return
	}

	// Clean the response with better validation
	cleanSQL := cleanSQLResponseImproved(sqlQuery, input)
	if cleanSQL == "" {
		fmt.Println("❌ Could not generate valid SQL from natural language")
		fmt.Println("💡 Try rephrasing your query or use direct SQL")
		return
	}

	fmt.Printf("📝 Generated SQL: %s\n", cleanSQL)
	
	// Ask user for confirmation
	fmt.Print("Execute this query? (y/n): ")
	reader := bufio.NewReader(os.Stdin)
	confirmation, _ := reader.ReadString('\n')
	confirmation = strings.TrimSpace(strings.ToLower(confirmation))
	
	if confirmation == "y" || confirmation == "yes" {
		// Parse and execute the generated SQL
		executeGeneratedSQL(cleanSQL, db)
	} else {
		fmt.Println("Query cancelled.")
	}
}

// Enhanced pattern matching fallback when Ollama is not available
func handleBasicPatternMatching(input string, db *schema.Database) {
	upperInput := strings.ToUpper(input)
	tables := db.GetAllTableNames()
	
	// Count queries
	if strings.Contains(upperInput, "HOW MANY") {
		for _, tableName := range tables {
			if strings.Contains(upperInput, strings.ToUpper(tableName)) {
				sqlQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s;", tableName)
				fmt.Printf("📝 Generated SQL: %s\n", sqlQuery)
				fmt.Print("Execute this query? (y/n): ")
				reader := bufio.NewReader(os.Stdin)
				confirmation, _ := reader.ReadString('\n')
				confirmation = strings.TrimSpace(strings.ToLower(confirmation))
				if confirmation == "y" || confirmation == "yes" {
					executeGeneratedSQL(sqlQuery, db)
				}
				return
			}
		}
	}
	
	// Show all records queries  
	if strings.Contains(upperInput, "SHOW ALL") || (strings.Contains(upperInput, "ALL") && !strings.Contains(upperInput, "DELETE")) {
		for _, tableName := range tables {
			if strings.Contains(upperInput, strings.ToUpper(tableName)) {
				sqlQuery := fmt.Sprintf("SELECT * FROM %s;", tableName)
				fmt.Printf("📝 Generated SQL: %s\n", sqlQuery)
				fmt.Print("Execute this query? (y/n): ")
				reader := bufio.NewReader(os.Stdin)
				confirmation, _ := reader.ReadString('\n')
				confirmation = strings.TrimSpace(strings.ToLower(confirmation))
				if confirmation == "y" || confirmation == "yes" {
					executeGeneratedSQL(sqlQuery, db)
				}
				return
			}
		}
	}
	
	// UPDATE patterns
	if strings.Contains(upperInput, "UPDATE") || strings.Contains(upperInput, "CHANGE") || 
	   strings.Contains(upperInput, "SET") || strings.Contains(upperInput, "MODIFY") {
		sqlQuery := handleUpdatePattern(input, db)
		if sqlQuery != "" {
			fmt.Printf("📝 Generated SQL: %s\n", sqlQuery)
			fmt.Print("Execute this query? (y/n): ")
			reader := bufio.NewReader(os.Stdin)
			confirmation, _ := reader.ReadString('\n')
			confirmation = strings.TrimSpace(strings.ToLower(confirmation))
			if confirmation == "y" || confirmation == "yes" {
				executeGeneratedSQL(sqlQuery, db)
			}
			return
		}
	}
	
	// DELETE patterns
	if strings.Contains(upperInput, "DELETE") || strings.Contains(upperInput, "REMOVE") {
		sqlQuery := handleDeletePattern(input, db)
		if sqlQuery != "" {
			fmt.Printf("📝 Generated SQL: %s\n", sqlQuery)
			fmt.Printf("⚠️  This will permanently delete data!\n")
			fmt.Print("Execute this query? (y/n): ")
			reader := bufio.NewReader(os.Stdin)
			confirmation, _ := reader.ReadString('\n')
			confirmation = strings.TrimSpace(strings.ToLower(confirmation))
			if confirmation == "y" || confirmation == "yes" {
				executeGeneratedSQL(sqlQuery, db)
			}
			return
		}
	}
	
	// DROP TABLE patterns
	if strings.Contains(upperInput, "DROP") && strings.Contains(upperInput, "TABLE") {
		sqlQuery := handleDropPattern(input, db)
		if sqlQuery != "" {
			fmt.Printf("📝 Generated SQL: %s\n", sqlQuery)
			fmt.Printf("⚠️  This will permanently delete the entire table and all its data!\n")
			fmt.Print("Execute this query? (y/n): ")
			reader := bufio.NewReader(os.Stdin)
			confirmation, _ := reader.ReadString('\n')
			confirmation = strings.TrimSpace(strings.ToLower(confirmation))
			if confirmation == "y" || confirmation == "yes" {
				executeGeneratedSQL(sqlQuery, db)
			}
			return
		}
	}
	
	fmt.Println("❌ Could not understand the query without Llama3")
	fmt.Println("💡 Please start Ollama service or use direct SQL commands")
	fmt.Println("💡 Examples:")
	fmt.Println("   SHOW TABLES; SELECT * FROM students;")
	fmt.Println("   UPDATE students SET course = 'CS' WHERE name = 'John';")
	fmt.Println("   DELETE FROM students WHERE id = 5;")
}

// Handle UPDATE natural language patterns
func handleUpdatePattern(input string, db *schema.Database) string {
	upperInput := strings.ToUpper(input)
	tables := db.GetAllTableNames()
	
	// Find table name
	var tableName string
	for _, table := range tables {
		if strings.Contains(upperInput, strings.ToUpper(table)) {
			tableName = table
			break
		}
	}
	
	if tableName == "" {
		return ""
	}
	
	// Pattern 1: "change/update [table] [column] to [value] where [condition]"
	// Example: "change student course to CS where name is John"
	if strings.Contains(upperInput, " TO ") && strings.Contains(upperInput, " WHERE ") {
		toIndex := strings.Index(upperInput, " TO ")
		whereIndex := strings.Index(upperInput, " WHERE ")
		
		if toIndex < whereIndex {
			// Extract column (word before "TO")
			beforeTo := input[:toIndex]
			words := strings.Fields(beforeTo)
			if len(words) >= 2 {
				column := words[len(words)-1] // Last word before "TO"
				
				// Extract value (between "TO" and "WHERE")
				value := strings.TrimSpace(input[toIndex+4:whereIndex])
				
				// Extract WHERE clause
				whereClause := strings.TrimSpace(input[whereIndex+7:])
				// Simple pattern: "name is John" -> "name = 'John'"
				if strings.Contains(strings.ToUpper(whereClause), " IS ") {
					parts := strings.Split(whereClause, " is ")
					if len(parts) == 2 {
						whereCol := strings.TrimSpace(parts[0])
						whereVal := strings.TrimSpace(parts[1])
						return fmt.Sprintf("UPDATE %s SET %s = '%s' WHERE %s = '%s';", tableName, column, value, whereCol, whereVal)
					}
				}
			}
		}
	}
	
	// Pattern 2: "update [name]'s [column] to [value]" OR "make [name]'s [column] as [value]"
	// Example: "update Abel's course to AI" OR "make sam's course as Data Science"
	if (strings.Contains(upperInput, "'S ") && strings.Contains(upperInput, " TO ")) ||
	   (strings.Contains(upperInput, "'S ") && strings.Contains(upperInput, " AS ")) {
		apostropheIndex := strings.Index(upperInput, "'S ")
		var toIndex int
		var keyword string
		
		if strings.Contains(upperInput, " TO ") {
			toIndex = strings.Index(upperInput, " TO ")
			keyword = " TO "
		} else {
			toIndex = strings.Index(upperInput, " AS ")
			keyword = " AS "
		}
		
		if apostropheIndex < toIndex {
			// Extract name
			beforeApostrophe := input[:apostropheIndex]
			words := strings.Fields(beforeApostrophe)
			if len(words) >= 1 {
				name := words[len(words)-1] // Last word before "'s"
				
				// Extract column (between "'s" and "to/as")
				column := strings.TrimSpace(input[apostropheIndex+3:toIndex])
				
				// Extract value (after "to/as")
				value := strings.TrimSpace(input[toIndex+len(keyword):])
				
				return fmt.Sprintf("UPDATE %s SET %s = '%s' WHERE name = '%s';", tableName, column, value, name)
			}
		}
	}
	
	// Pattern 3: "set [column] to [value] for [table] where [condition]"
	if strings.Contains(upperInput, "SET ") && strings.Contains(upperInput, " FOR ") && strings.Contains(upperInput, " WHERE ") {
		setIndex := strings.Index(upperInput, "SET ")
		forIndex := strings.Index(upperInput, " FOR ")
		whereIndex := strings.Index(upperInput, " WHERE ")
		
		if setIndex < forIndex && forIndex < whereIndex {
			// Extract column and value
			setPart := input[setIndex+4:forIndex]
			if strings.Contains(setPart, " to ") {
				parts := strings.Split(setPart, " to ")
				if len(parts) == 2 {
					column := strings.TrimSpace(parts[0])
					value := strings.TrimSpace(parts[1])
					
					// Extract WHERE clause
					whereClause := strings.TrimSpace(input[whereIndex+7:])
					if strings.Contains(strings.ToUpper(whereClause), " IS ") {
						whereParts := strings.Split(whereClause, " is ")
						if len(whereParts) == 2 {
							whereCol := strings.TrimSpace(whereParts[0])
							whereVal := strings.TrimSpace(whereParts[1])
							return fmt.Sprintf("UPDATE %s SET %s = '%s' WHERE %s = '%s';", tableName, column, value, whereCol, whereVal)
						}
					}
				}
			}
		}
	}
	
	return ""
}

// Handle DELETE natural language patterns
func handleDeletePattern(input string, db *schema.Database) string {
	upperInput := strings.ToUpper(input)
	tables := db.GetAllTableNames()
	
	// Find table name
	var tableName string
	for _, table := range tables {
		if strings.Contains(upperInput, strings.ToUpper(table)) {
			tableName = table
			break
		}
	}
	
	if tableName == "" {
		return ""
	}
	
	// Pattern 1: "delete [table] where [condition]"
	// Example: "delete student where name is John"
	if strings.Contains(upperInput, " WHERE ") {
		whereIndex := strings.Index(upperInput, " WHERE ")
		whereClause := strings.TrimSpace(input[whereIndex+7:])
		
		if strings.Contains(strings.ToUpper(whereClause), " IS ") {
			parts := strings.Split(whereClause, " is ")
			if len(parts) == 2 {
				whereCol := strings.TrimSpace(parts[0])
				whereVal := strings.TrimSpace(parts[1])
				return fmt.Sprintf("DELETE FROM %s WHERE %s = '%s';", tableName, whereCol, whereVal)
			}
		}
	}
	
	// Pattern 2: "remove [table] with [column] [value]"
	// Example: "remove student with id 5"
	if strings.Contains(upperInput, " WITH ") {
		withIndex := strings.Index(upperInput, " WITH ")
		withClause := strings.TrimSpace(input[withIndex+6:])
		words := strings.Fields(withClause)
		
		if len(words) >= 2 {
			column := words[0]
			value := words[1]
			return fmt.Sprintf("DELETE FROM %s WHERE %s = '%s';", tableName, column, value)
		}
	}
	
	// Pattern 3: "delete all records from [table] where [condition]"
	if strings.Contains(upperInput, "ALL RECORDS") || strings.Contains(upperInput, "ALL") {
		if strings.Contains(upperInput, " WHERE ") {
			whereIndex := strings.Index(upperInput, " WHERE ")
			whereClause := strings.TrimSpace(input[whereIndex+7:])
			
			if strings.Contains(strings.ToUpper(whereClause), " IS ") {
				parts := strings.Split(whereClause, " is ")
				if len(parts) == 2 {
					whereCol := strings.TrimSpace(parts[0])
					whereVal := strings.TrimSpace(parts[1])
					return fmt.Sprintf("DELETE FROM %s WHERE %s = '%s';", tableName, whereCol, whereVal)
				}
			}
		}
	}
	
	return ""
}

// Handle DROP TABLE natural language patterns
func handleDropPattern(input string, db *schema.Database) string {
	upperInput := strings.ToUpper(input)
	tables := db.GetAllTableNames()
	
	// Pattern 1: "drop table [table_name]"
	// Pattern 2: "delete table [table_name]"
	// Pattern 3: "remove table [table_name]"
	// Pattern 4: "drop the [table_name] table"
	
	for _, table := range tables {
		if strings.Contains(upperInput, strings.ToUpper(table)) {
			return fmt.Sprintf("DROP TABLE %s;", table)
		}
	}
	
	return ""
}

// Enhanced Ollama query with better parameters
func queryOllamaImproved(prompt string) (string, error) {
	url := "http://localhost:11434/api/generate"
	
	requestBody := map[string]interface{}{
		"model":  "llama3",
		"prompt": prompt,
		"stream": false,
		"options": map[string]interface{}{
			"temperature":     0.1,    // Very low for consistent SQL
			"top_p":          0.9,
			"repeat_penalty": 1.1,
			"num_predict":    100,     // Limit response length
			"stop":           []string{"\n\n", "Explanation:", "Note:"},
		},
	}
	
	jsonData, err := json.Marshal(requestBody)
	if err != nil {
		return "", err
	}
	
	// Create HTTP client with timeout
	client := &http.Client{
		Timeout: 30 * time.Second,
	}
	
	resp, err := client.Post(url, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		return "", fmt.Errorf("failed to connect to Ollama (is it running?): %w", err)
	}
	defer resp.Body.Close()
	
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("Ollama returned status %d", resp.StatusCode)
	}
	
	var response OllamaResponse
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return "", fmt.Errorf("failed to parse Ollama response: %w", err)
	}
	
	return response.Response, nil
}

// Build database context for Llama3 with enhanced information
func buildDatabaseContext(db *schema.Database) string {
	tables := db.GetAllTableNames()
	
	context := "AVAILABLE COMMANDS:\n"
	context += "- SHOW TABLES; (to list all tables)\n"
	context += "- SELECT * FROM table_name; (to get all records)\n"
	context += "- SELECT column FROM table_name WHERE condition; (to filter records)\n"
	context += "- SELECT COUNT(*) FROM table_name; (to count records)\n"
	context += "- INSERT INTO table_name (columns) VALUES (values); (to add records)\n"
	context += "- UPDATE table_name SET column = 'value' WHERE condition; (to modify records)\n"
	context += "- DELETE FROM table_name WHERE condition; (to remove records)\n"
	context += "- DROP TABLE table_name; (to delete entire table)\n\n"
	
	if len(tables) == 0 {
		context += "TABLES: No tables exist in the database.\n"
		return context
	}
	
	context += "AVAILABLE TABLES:\n"
	for _, tableName := range tables {
		if table, exists := db.GetTable(tableName); exists {
			context += fmt.Sprintf("- %s: ", tableName)
			columnInfo := []string{}
			for _, col := range table.Columns {
				columnInfo = append(columnInfo, fmt.Sprintf("%s(%s)", col.Name, strings.ToLower(string(col.Type))))
			}
			context += strings.Join(columnInfo, ", ") + "\n"
		}
	}
	
	return context
}

// Improved SQL response cleaning with query type detection
func cleanSQLResponseImproved(response, originalQuery string) string {
	response = strings.TrimSpace(response)
	
	// Remove markdown and formatting
	response = strings.TrimPrefix(response, "```sql")
	response = strings.TrimPrefix(response, "```")
	response = strings.TrimSuffix(response, "```")
	
	// Handle common natural language patterns first
	upperQuery := strings.ToUpper(originalQuery)
	
	// Table listing queries - direct mapping
	if strings.Contains(upperQuery, "TABLES") || 
	   strings.Contains(upperQuery, "WHAT TABLES") ||
	   strings.Contains(upperQuery, "WHICH TABLES") ||
	   strings.Contains(upperQuery, "SHOW TABLES") ||
	   strings.Contains(upperQuery, "LIST TABLES") ||
	   strings.Contains(upperQuery, "ALL TABLES") ||
	   (strings.Contains(upperQuery, "WHAT") && strings.Contains(upperQuery, "HAVE")) ||
	   (strings.Contains(upperQuery, "WHICH") && (strings.Contains(upperQuery, "DATABASE") || strings.Contains(upperQuery, "TABLES"))) {
		return "SHOW TABLES;"
	}
	
	// Extract SQL from response
	lines := strings.Split(response, "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		
		// Skip explanatory text
		upper := strings.ToUpper(line)
		if strings.Contains(upper, "THIS QUERY") ||
		   strings.Contains(upper, "THE SQL") ||
		   strings.Contains(upper, "EXPLANATION") ||
		   strings.Contains(upper, "HERE IS") ||
		   strings.Contains(upper, "ANSWER:") ||
		   strings.Contains(upper, "BASED ON") ||
		   strings.Contains(upper, "TO ") && strings.Contains(upper, "USE:") {
			continue
		}
		
		// Find valid SQL commands
		if strings.Contains(upper, "SELECT") ||
		   strings.Contains(upper, "INSERT") ||
		   strings.Contains(upper, "UPDATE") ||
		   strings.Contains(upper, "DELETE") ||
		   strings.Contains(upper, "CREATE") ||
		   strings.Contains(upper, "DROP") ||
		   strings.Contains(upper, "SHOW") {
			// Ensure it ends with semicolon
			if !strings.HasSuffix(line, ";") {
				line += ";"
			}
			return line
		}
	}
	
	// Fallback: return cleaned response
	if !strings.HasSuffix(response, ";") {
		response += ";"
	}
	return response
}

// Separate function to execute generated SQL with better error handling
func executeGeneratedSQL(sqlQuery string, db *schema.Database) {
	// Special handling for SHOW TABLES
	if strings.ToUpper(strings.TrimSpace(sqlQuery)) == "SHOW TABLES;" {
		fmt.Println("✅ Executing query...")
		tableNames := db.GetAllTableNames()
		if len(tableNames) == 0 {
			fmt.Println("No tables found.")
		} else {
			fmt.Println("Tables:")
			for _, name := range tableNames {
				fmt.Printf("- %s\n", name)
			}
		}
		return
	}
	
	// Parse and execute other SQL commands
	cmd, perr := parser.Parse(sqlQuery)
	if perr != nil {
		fmt.Printf("❌ Generated SQL parse error: %s\n", perr)
		fmt.Printf("💡 Generated SQL was: %s\n", sqlQuery)
		fmt.Println("💡 Try rephrasing your natural language query")
		return
	}
	
	fmt.Println("✅ Executing query...")
	executeCommand(cmd, db)
}

// Execute parsed SQL command
func executeCommand(cmd parser.Command, db *schema.Database) {
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

	case "INSERT":
		out, err := handlers.HandleInsertWithImages(cmd, db, imageDirectory)
		if err != nil {
			fmt.Println("INSERT error:", err)
		} else {
			fmt.Println(out)
		}

	case "CREATE":
		if len(parts) < 3 || strings.ToUpper(parts[1]) != "TABLE" {
			fmt.Println("Invalid CREATE TABLE syntax. Example: CREATE TABLE users (id INT, name TEXT);")
			return
		}

		tableName := strings.TrimSpace(parts[2])
		
		// Parse column definitions
		fullCommand := strings.Join(parts, " ")
		openParen := strings.Index(fullCommand, "(")
		closeParen := strings.LastIndex(fullCommand, ")")
		
		if openParen == -1 || closeParen == -1 || closeParen <= openParen {
			fmt.Println("Invalid CREATE TABLE syntax. Missing column definitions.")
			return
		}
		
		colsStr := fullCommand[openParen+1 : closeParen]
		colDefs := strings.Split(colsStr, ",")
		
		var columns []schema.Column
		for _, colDef := range colDefs {
			colParts := strings.Fields(strings.TrimSpace(colDef))
			if len(colParts) != 2 {
				fmt.Printf("Invalid column definition: %s\n", colDef)
				return
			}
			
			colName := strings.TrimSpace(colParts[0])
			colType := strings.ToUpper(strings.TrimSpace(colParts[1]))
			
			if !schema.ValidateColumnType(colType) {
				fmt.Printf("Invalid column type: %s. Supported types: INT, TEXT, DECIMAL, BOOL, IMAGE\n", colType)
				return
			}
			
			columns = append(columns, schema.Column{
				Name: colName,
				Type: schema.DataType(colType),
			})
		}
		
		if len(columns) == 0 {
			fmt.Println("No valid columns defined.")
			return
		}
		
		table := schema.Table{
			Name:    tableName,
			Columns: columns,
		}
		
		if err := db.AddTable(table); err != nil {
			fmt.Printf("Error creating table: %s\n", err)
		} else {
			fmt.Printf("✅ Table '%s' created successfully.\n", tableName)
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
			return
		}
		tableName := strings.TrimSpace(parts[2])

		if err := db.RemoveTable(tableName); err != nil {
			fmt.Printf("Error dropping table from schema: %s\n", err)
			return
		}

		tableFile, err := storage.NewTableFile(db.GetDBPath(), tableName)
		if err != nil {
			fmt.Printf("Warning: Could not get table file for '%s' to delete: %s\n", tableName, err)
		} else {
			if err := tableFile.DeleteFile(); err != nil {
				fmt.Printf("Warning: Could not delete data file for '%s': %s\n", tableName, err)
			}
		}
		fmt.Printf("✅ Table '%s' dropped successfully.\n", tableName)

	case "UPDATE":
		out, err := handlers.HandleUpdate(cmd, db)
		if err != nil {
			fmt.Println("UPDATE error:", err)
		} else {
			fmt.Println(out)
		}

	case "DELETE":
		out, err := handlers.HandleDelete(cmd, db)
		if err != nil {
			fmt.Println("DELETE error:", err)
		} else {
			fmt.Println(out)
		}

	default:
		fmt.Printf("❌ Unknown SQL command: %s\n", command)
		fmt.Println("💡 Supported commands: SELECT, INSERT, CREATE TABLE, DROP TABLE, SHOW TABLES")
	}
}

// Function to handle setting image directory
func handleSetImageDir(input string) {
	parts := strings.Split(input, " ")
	if len(parts) < 4 {
		fmt.Println("Invalid syntax. Use: SET IMAGE DIR 'path/to/images'")
		return
	}
	
	newDir := strings.Join(parts[3:], " ")
	newDir = strings.Trim(newDir, "'\"")
	
	if _, err := os.Stat(newDir); os.IsNotExist(err) {
		fmt.Printf("Warning: Directory '%s' does not exist.\n", newDir)
		return
	}
	
	imageDirectory = newDir
	fmt.Printf("✅ Image directory set to: %s\n", imageDirectory)
}

// Function to find image path based on identifier
func findImagePath(identifier string) string {
	if imageDirectory == "" {
		return ""
	}
	
	imageExts := []string{".jpg", ".jpeg", ".png", ".gif", ".bmp", ".tiff", ".webp"}
	
	// Try exact match first
	for _, ext := range imageExts {
		fullPath := filepath.Join(imageDirectory, identifier+ext)
		if _, err := os.Stat(fullPath); err == nil {
			return fullPath
		}
	}
	
	// Try partial match (file containing the identifier)
	files, err := os.ReadDir(imageDirectory)
	if err != nil {
		return ""
	}
	
	for _, file := range files {
		if !file.IsDir() && strings.Contains(file.Name(), identifier) {
			for _, ext := range imageExts {
				if strings.HasSuffix(strings.ToLower(file.Name()), ext) {
					return filepath.Join(imageDirectory, file.Name())
				}
			}
		}
	}
	
	return ""
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

// Coerce value function
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
	case schema.Image:
		// For image type, find the actual image path
		cleanedIdentifier := strings.Trim(trimmedVal, "'\"")
		imagePath := findImagePath(cleanedIdentifier)
		if imagePath == "" {
			return "", fmt.Errorf("image file not found for identifier: %s", cleanedIdentifier)
		}
		return imagePath, nil
	default:
		return nil, fmt.Errorf("unsupported data type for coercion: %s", targetType)
	}
}