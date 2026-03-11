package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"Custom_DB/pkg/handlers"
	"Custom_DB/pkg/importer"
	"Custom_DB/pkg/parser"
	"Custom_DB/pkg/schema"
	"Custom_DB/pkg/storage"
)

var db *schema.Database

// nlColEntry pairs a column's actual name with its space-normalized uppercase form.
type nlColEntry struct {
	name       string
	normalized string
}

// nlValueInfo maps a sampled data value back to its column.
type nlValueInfo struct {
	col      string
	original string
}

type QueryRequest struct {
	Query     string `json:"query"`
	IsNatural bool   `json:"isNatural"`
	ConversationID string `json:"conversationId"`
}

type OllamaRequest struct {
	Model   string                 `json:"model"`
	Prompt  string                 `json:"prompt"`
	Stream  bool                   `json:"stream"`
	Options map[string]interface{} `json:"options"`
}

type OllamaResponse struct {
	Response string `json:"response"`
}

type QueryResponse struct {
	Success      bool   `json:"success"`
	Result       string `json:"result,omitempty"`
	Error        string `json:"error,omitempty"`
	GeneratedSQL string `json:"generatedSQL,omitempty"`
}

type TableInfo struct {
	Name    string          `json:"name"`
	Columns []schema.Column `json:"columns"`
}

type TablesResponse struct {
	Success bool        `json:"success"`
	Tables  []TableInfo `json:"tables"`
}

// -- Conversation persistence --------------------------------------------------

const convsDir = "data/conversations"

type ConvMessage struct {
	Role      string `json:"role"` // "user" | "bot"
	Text      string `json:"text"`
	SQL       string `json:"sql,omitempty"`
	Result    string `json:"result,omitempty"`
	Error     string `json:"error,omitempty"`
	Timestamp string `json:"timestamp"`
}

type Conversation struct {
	ID        string        `json:"id"`
	Title     string        `json:"title"`
	CreatedAt string        `json:"createdAt"`
	UpdatedAt string        `json:"updatedAt"`
	Messages  []ConvMessage `json:"messages"`
}

func convPath(id string) string { return filepath.Join(convsDir, id+".json") }

func loadConv(id string) (*Conversation, error) {
	data, err := os.ReadFile(convPath(id))
	if err != nil {
		return nil, err
	}
	var c Conversation
	return &c, json.Unmarshal(data, &c)
}

func saveConv(c *Conversation) error {
	if err := os.MkdirAll(convsDir, 0755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(convPath(c.ID), data, 0644)
}

func listConvs() ([]Conversation, error) {
	entries, err := os.ReadDir(convsDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var convs []Conversation
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		id := strings.TrimSuffix(e.Name(), ".json")
		c, err := loadConv(id)
		if err != nil {
			continue
		}
		// Return summary (no messages) for the list view
		convs = append(convs, Conversation{
			ID:        c.ID,
			Title:     c.Title,
			CreatedAt: c.CreatedAt,
			UpdatedAt: c.UpdatedAt,
		})
	}
	// Sort newest first
	sort.Slice(convs, func(i, j int) bool {
		return convs[i].UpdatedAt > convs[j].UpdatedAt
	})
	return convs, nil
}

// deriveTitle builds a short title from the first user message.
func deriveTitle(msg string) string {
	words := strings.Fields(msg)
	if len(words) > 8 {
		words = words[:8]
	}
	title := strings.Join(words, " ")
	if len(title) > 60 {
		title = title[:57] + "�"
	}
	return title
}

func main() {
	var err error
	db, err = schema.NewDatabase("data/my_first_db")
	if err != nil {
		log.Fatalf("Failed to load database: %s", err)
	}

	http.Handle("/static/", http.StripPrefix("/static/", http.FileServer(http.Dir("web/static"))))
	http.HandleFunc("/", serveHome)
	http.HandleFunc("/api/query", handleQuery)
	http.HandleFunc("/api/tables", handleTables)
	http.HandleFunc("/api/upload", handleUpload)
	http.HandleFunc("/api/conversations", handleConversations)
	http.HandleFunc("/api/conversations/", handleConversationByID)

	fmt.Println("CustomDB Web UI running at http://localhost:8082")
	log.Fatal(http.ListenAndServe(":8082", nil))
}

func serveHome(w http.ResponseWriter, r *http.Request) {
	http.ServeFile(w, r, "web/index.html")
}

func handleTables(w http.ResponseWriter, r *http.Request) {
	var tables []TableInfo
	for name, table := range db.Tables {
		tables = append(tables, TableInfo{Name: name, Columns: table.Columns})
	}
	writeJSON(w, TablesResponse{Success: true, Tables: tables})
}

// GET  /api/conversations       ? list all conversations (no messages)
// POST /api/conversations        ? create new conversation
func handleConversations(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		convs, err := listConvs()
		if err != nil {
			writeJSON(w, map[string]interface{}{"success": false, "error": err.Error()})
			return
		}
		writeJSON(w, map[string]interface{}{"success": true, "conversations": convs})

	case http.MethodPost:
		id := fmt.Sprintf("%d", time.Now().UnixMilli())
		now := time.Now().Format(time.RFC3339)
		c := &Conversation{
			ID:        id,
			Title:     "New Chat",
			CreatedAt: now,
			UpdatedAt: now,
			Messages:  []ConvMessage{},
		}
		if err := saveConv(c); err != nil {
			writeJSON(w, map[string]interface{}{"success": false, "error": err.Error()})
			return
		}
		writeJSON(w, map[string]interface{}{"success": true, "conversation": c})

	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// GET    /api/conversations/{id}            ? load full conversation
// POST   /api/conversations/{id}/message     ? append a message pair
// DELETE /api/conversations/{id}            ? delete conversation
// PATCH  /api/conversations/{id}            ? update title
func handleConversationByID(w http.ResponseWriter, r *http.Request) {
	// Parse path: /api/conversations/{id}  or  /api/conversations/{id}/message
	path := strings.TrimPrefix(r.URL.Path, "/api/conversations/")
	parts := strings.SplitN(path, "/", 2)
	id := parts[0]
	sub := ""
	if len(parts) == 2 {
		sub = parts[1]
	}

	if id == "" {
		http.Error(w, "missing id", http.StatusBadRequest)
		return
	}

	switch {
	case r.Method == http.MethodGet && sub == "":
		c, err := loadConv(id)
		if err != nil {
			writeJSON(w, map[string]interface{}{"success": false, "error": "conversation not found"})
			return
		}
		writeJSON(w, map[string]interface{}{"success": true, "conversation": c})

	case r.Method == http.MethodPost && sub == "message":
		// Body: { userText, botData: {success, result, error, generatedSQL} }
		var body struct {
			UserText string `json:"userText"`
			BotData  struct {
				Success      bool   `json:"success"`
				Result       string `json:"result"`
				Error        string `json:"error"`
				GeneratedSQL string `json:"generatedSQL"`
			} `json:"botData"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			writeJSON(w, map[string]interface{}{"success": false, "error": "bad request"})
			return
		}
		c, err := loadConv(id)
		if err != nil {
			writeJSON(w, map[string]interface{}{"success": false, "error": "conversation not found"})
			return
		}
		now := time.Now().Format(time.RFC3339)
		c.Messages = append(c.Messages, ConvMessage{
			Role:      "user",
			Text:      body.UserText,
			Timestamp: now,
		})
		c.Messages = append(c.Messages, ConvMessage{
			Role:      "bot",
			SQL:       body.BotData.GeneratedSQL,
			Result:    body.BotData.Result,
			Error:     body.BotData.Error,
			Timestamp: now,
		})
		// Auto-title from first user message
		if c.Title == "New Chat" && body.UserText != "" {
			c.Title = deriveTitle(body.UserText)
		}
		c.UpdatedAt = now
		if err := saveConv(c); err != nil {
			writeJSON(w, map[string]interface{}{"success": false, "error": err.Error()})
			return
		}
		writeJSON(w, map[string]interface{}{"success": true})

	case r.Method == http.MethodPatch && sub == "":
		var body struct {
			Title string `json:"title"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Title == "" {
			writeJSON(w, map[string]interface{}{"success": false, "error": "bad request"})
			return
		}
		c, err := loadConv(id)
		if err != nil {
			writeJSON(w, map[string]interface{}{"success": false, "error": "conversation not found"})
			return
		}
		c.Title = body.Title
		c.UpdatedAt = time.Now().Format(time.RFC3339)
		if err := saveConv(c); err != nil {
			writeJSON(w, map[string]interface{}{"success": false, "error": err.Error()})
			return
		}
		writeJSON(w, map[string]interface{}{"success": true})

	case r.Method == http.MethodDelete && sub == "":
		if err := os.Remove(convPath(id)); err != nil && !os.IsNotExist(err) {
			writeJSON(w, map[string]interface{}{"success": false, "error": err.Error()})
			return
		}
		writeJSON(w, map[string]interface{}{"success": true})

	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func handleUpload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}

	if err := r.ParseMultipartForm(64 << 20); err != nil {
		writeJSON(w, QueryResponse{Success: false, Error: "failed to parse upload: " + err.Error()})
		return
	}

	tableName := strings.TrimSpace(r.FormValue("table_name"))
	if tableName == "" {
		writeJSON(w, QueryResponse{Success: false, Error: "table_name is required"})
		return
	}

	file, header, err := r.FormFile("file")
	if err != nil {
		writeJSON(w, QueryResponse{Success: false, Error: "no file received"})
		return
	}
	defer file.Close()

	ext := strings.ToLower(filepath.Ext(header.Filename))
	if ext != ".csv" && ext != ".parquet" {
		writeJSON(w, QueryResponse{Success: false, Error: "unsupported file type '" + ext + "' � only .csv and .parquet are supported"})
		return
	}

	tmpf, err := os.CreateTemp("", "customdb_upload_*"+ext)
	if err != nil {
		writeJSON(w, QueryResponse{Success: false, Error: "failed to create temp file"})
		return
	}
	defer os.Remove(tmpf.Name())

	if _, err := io.Copy(tmpf, file); err != nil {
		tmpf.Close()
		writeJSON(w, QueryResponse{Success: false, Error: "failed to save uploaded file"})
		return
	}
	tmpf.Close()

	var importErr error
	switch ext {
	case ".csv":
		importErr = importer.ImportCSV(tmpf.Name(), db, tableName)
	case ".parquet":
		importErr = importer.ImportParquet(tmpf.Name(), db, tableName)
	}

	if importErr != nil {
		writeJSON(w, QueryResponse{Success: false, Error: importErr.Error()})
		return
	}

	writeJSON(w, QueryResponse{
		Success: true,
		Result:  fmt.Sprintf("Imported '%s' into table '%s' successfully.", header.Filename, tableName),
	})
}

func handleQuery(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}

	var req QueryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, QueryResponse{Success: false, Error: "invalid request"})
		return
	}

	query := strings.TrimSpace(req.Query)
	query = strings.TrimSuffix(query, ";")

	if query == "" {
		writeJSON(w, QueryResponse{Success: false, Error: "empty query"})
		return
	}

	// Natural language mode or auto-detect
	var prevTable string
	if req.ConversationID != "" {
		if c, err := loadConv(req.ConversationID); err == nil {
			for i := len(c.Messages) - 1; i >= 0; i-- {
				if c.Messages[i].Role == "bot" && c.Messages[i].SQL != "" {
					upperSQL := strings.ToUpper(c.Messages[i].SQL)
					tables := db.GetAllTableNames()
					for _, t := range tables {
						if strings.Contains(upperSQL, strings.ToUpper(t)) {
							prevTable = t
							break
						}
					}
					if prevTable != "" {
						break
					}
				}
			}
		}
	}

	if req.IsNatural || isNaturalLanguage(query) {
		sql, err := convertToSQL(query, prevTable)
		if err != nil {
			writeJSON(w, QueryResponse{Success: false, Error: err.Error()})
			return
		}
		cmd, perr := parser.Parse(strings.TrimSuffix(sql, ";"))
		if perr != nil {
			writeJSON(w, QueryResponse{Success: false, Error: perr.Error(), GeneratedSQL: sql})
			return
		}
		result, execErr := runCommand(cmd)
		if execErr != nil {
			writeJSON(w, QueryResponse{Success: false, Error: execErr.Error(), GeneratedSQL: sql})
			return
		}
		writeJSON(w, QueryResponse{Success: true, Result: result, GeneratedSQL: sql})
		return
	}

	cmd, err := parser.Parse(query)
	if err != nil {
		writeJSON(w, QueryResponse{Success: false, Error: "Parse error: " + err.Error()})
		return
	}

	result, execErr := runCommand(cmd)
	if execErr != nil {
		writeJSON(w, QueryResponse{Success: false, Error: execErr.Error()})
		return
	}
	writeJSON(w, QueryResponse{Success: true, Result: result})
}

func runCommand(cmd parser.Command) (string, error) {
	switch cmd.Type {
	case "SELECT":
		return handlers.HandleSelect(cmd, db)

	case "INSERT":
		return handlers.HandleInsert(cmd, db)

	case "UPDATE":
		return handlers.HandleUpdate(cmd, db)

	case "DELETE":
		return handlers.HandleDelete(cmd, db)

	case "SHOW":
		if len(cmd.Tokens) > 1 && strings.ToUpper(cmd.Tokens[1]) == "TABLES" {
			names := db.GetAllTableNames()
			if len(names) == 0 {
				return "No tables found.", nil
			}
			var sb strings.Builder
			sb.WriteString("Tables:\n")
			for _, name := range names {
				sb.WriteString(fmt.Sprintf("- %s\n", name))
			}
			return sb.String(), nil
		}
		return "", fmt.Errorf("unknown SHOW command")

	case "CREATE":
		parts := cmd.Tokens
		if len(parts) < 3 || strings.ToUpper(parts[1]) != "TABLE" {
			return "", fmt.Errorf("invalid CREATE TABLE syntax. Example: CREATE TABLE users (id INT, name TEXT)")
		}
		tableName := strings.TrimSpace(parts[2])
		full := strings.Join(parts, " ")
		openParen := strings.Index(full, "(")
		closeParen := strings.LastIndex(full, ")")
		if openParen == -1 || closeParen == -1 || closeParen <= openParen {
			return "", fmt.Errorf("missing column definitions in CREATE TABLE")
		}
		colsStr := full[openParen+1 : closeParen]
		colDefs := strings.Split(colsStr, ",")
		var columns []schema.Column
		for _, colDef := range colDefs {
			colParts := strings.Fields(strings.TrimSpace(colDef))
			if len(colParts) != 2 {
				return "", fmt.Errorf("invalid column definition: %s", colDef)
			}
			colType := strings.ToUpper(colParts[1])
			if !schema.ValidateColumnType(colType) {
				return "", fmt.Errorf("invalid column type: %s (supported: INT, TEXT, DECIMAL, BOOL, IMAGE)", colType)
			}
			columns = append(columns, schema.Column{Name: colParts[0], Type: schema.DataType(colType)})
		}
		if len(columns) == 0 {
			return "", fmt.Errorf("no columns defined")
		}
		if err := db.AddTable(schema.Table{Name: tableName, Columns: columns}); err != nil {
			return "", err
		}
		return fmt.Sprintf("Table '%s' created successfully.", tableName), nil

	case "DROP":
		parts := cmd.Tokens
		if len(parts) < 3 || strings.ToUpper(parts[1]) != "TABLE" {
			return "", fmt.Errorf("invalid DROP TABLE syntax. Example: DROP TABLE users")
		}
		tableName := strings.TrimSpace(parts[2])
		if err := db.RemoveTable(tableName); err != nil {
			return "", err
		}
		tf, err := storage.NewTableFile(db.GetDBPath(), tableName)
		if err == nil {
			tf.DeleteFile()
		}
		return fmt.Sprintf("Table '%s' dropped successfully.", tableName), nil

	default:
		return "", fmt.Errorf("unknown command: %s. Supported: SELECT, INSERT, UPDATE, DELETE, CREATE TABLE, DROP TABLE, SHOW TABLES", cmd.Type)
	}
}

// isNaturalLanguage returns true when the input is not a SQL keyword
func isNaturalLanguage(input string) bool {
	upper := strings.ToUpper(strings.TrimSpace(input))
	for _, kw := range []string{"SELECT ", "INSERT ", "UPDATE ", "DELETE ", "CREATE ", "DROP ", "SHOW TABLES"} {
		if strings.HasPrefix(upper, kw) || upper == strings.TrimSpace(kw) {
			return false
		}
	}
	return true
}

// convertToSQL converts NL to SQL: tries pattern matching first, then Ollama
func convertToSQL(input string, prevTable string) (string, error) {
	upper := strings.ToUpper(input)
	tables := db.GetAllTableNames()

	// SHOW TABLES patterns � only when query is specifically about schema/table listing
	if (strings.Contains(upper, "TABLE") || strings.Contains(upper, "DATABASE")) &&
		!strings.Contains(upper, "FIND") && !strings.Contains(upper, "SHOW ME") &&
		!strings.Contains(upper, "CUSTOMER") && !strings.Contains(upper, "ACCOUNT") &&
		!strings.Contains(upper, "BALANCE") && !strings.Contains(upper, "SPEND") &&
		!strings.Contains(upper, "LOAN") && !strings.Contains(upper, "CREDIT") {
		return "SHOW TABLES;", nil
	}

	// Find best-matching table using column and domain keywords
	tableName := findBestMatchingTable(upper, tables, prevTable)

	// COUNT patterns
	if strings.Contains(upper, "HOW MANY") || strings.Contains(upper, "COUNT") {
		if tableName != "" {
			return fmt.Sprintf("SELECT COUNT(*) FROM %s;", tableName), nil
		}
	}

	if tableName != "" {
		whereClause := extractWhereCondition(input, tableName)
		if whereClause != "" {
			return fmt.Sprintf("SELECT * FROM %s WHERE %s;", tableName, whereClause), nil
		}
		return fmt.Sprintf("SELECT * FROM %s;", tableName), nil
	}

	// Try Ollama
	sql, err := queryOllama(input)
	if err != nil {
		return "", fmt.Errorf("could not understand query; Ollama not available (%s). Try SQL directly, e.g. SELECT * FROM %s;", err.Error(), func() string {
			if len(tables) > 0 {
				return tables[0]
			}
			return "tablename"
		}())
	}
	return cleanSQL(sql, input), nil
}

// findBestMatchingTable returns the table name that best matches the natural language query.
// It first tries direct name match, then column-keyword scoring, then domain hints.
func findBestMatchingTable(upper string, tables []string, prevTable string) string {
	// 0. Follow-up / context queries ("among this", "from these"�) reuse the last table.
	for _, ind := range []string{
		"AMONG THIS", "AMONG THESE", "FROM THIS", "FROM THESE",
		"IN THIS", "IN THESE", "OF THIS", "OF THESE",
		"WITHIN THIS", "WITHIN THESE",
	} {
		if strings.Contains(upper, ind) && prevTable != "" {
			return prevTable
		}
	}

	// 1. Direct table name substring match
	for _, t := range tables {
		if strings.Contains(upper, strings.ToUpper(t)) {
			return t
		}
	}

	// 2. Score each table by how many of its column name words appear in the query
	bestTable := ""
	bestScore := 0
	for _, t := range tables {
		tbl, ok := db.GetTable(t)
		if !ok {
			continue
		}
		score := 0
		for _, col := range tbl.Columns {
			for _, part := range strings.Split(col.Name, "_") {
				if len(part) > 3 && strings.Contains(upper, strings.ToUpper(part)) {
					score++
				}
			}
		}
		if score > bestScore {
			bestScore = score
			bestTable = t
		}
	}
	if bestScore > 0 {
		return bestTable
	}

	// 3. Domain keyword hints when column scoring yields nothing
	type hint struct {
		keywords []string
		table    string
	}
	hints := []hint{
		{[]string{"CUSTOMER", "BANKING", "ACCOUNT", "LOAN", "CREDIT", "SALARY", "INCOME", "FRAUD", "TRANSACTION"}, "banking"},
		{[]string{"STUDENT"}, "students"},
		{[]string{"TEACHER"}, "teacher"},
		{[]string{"PHOTO", "IMAGE", "PICTURE"}, "photos"},
	}
	for _, h := range hints {
		for _, kw := range h.keywords {
			if strings.Contains(upper, kw) {
				for _, t := range tables {
					if t == h.table {
						return t
					}
				}
			}
		}
	}

	// 4. Default to previous table context if no strong matching keyword
	if prevTable != "" {
		for _, t := range tables {
			if t == prevTable {
				return prevTable
			}
		}
	}
	
	return ""
}

// extractWhereCondition tries to extract a SQL WHERE clause from natural language.
func extractWhereCondition(input string, tableName string) string {
	tbl, ok := db.GetTable(tableName)
	if !ok {
		return ""
	}
	upper := strings.ToUpper(input)

	var cols []nlColEntry
	for _, col := range tbl.Columns {
		cols = append(cols, nlColEntry{
			name:       col.Name,
			normalized: strings.ToUpper(strings.ReplaceAll(col.Name, "_", " ")),
		})
	}

	type opPattern struct {
		phrase string
		op     string
	}
	opPatterns := []opPattern{
		{"MORE THAN", ">"},
		{"GREATER THAN", ">"},
		{"ABOVE", ">"},
		{"OVER", ">"},
		{"EXCEEDS", ">"},
		{"EXCEED", ">"},
		{"AT LEAST", ">="},
		{"LESS THAN", "<"},
		{"BELOW", "<"},
		{"UNDER", "<"},
		{"AT MOST", "<="},
		{"EQUAL TO", "="},
		{"EQUALS", "="},
		{"IS", "="},
	}

	words := strings.Fields(input)
	for i, word := range words {
		num := parseNumericWord(word)
		if num == "" {
			continue
		}
		contextBefore := strings.ToUpper(strings.Join(words[:i], " "))

		op := ""
		for _, opPat := range opPatterns {
			if strings.Contains(contextBefore, opPat.phrase) {
				op = opPat.op
				break
			}
		}
		if op == "" {
			op = ">"
		}

		colName := matchColumnInContext(contextBefore, cols)
		if colName == "" {
			colName = matchColumnInContext(upper, cols)
		}
		if colName != "" {
			return fmt.Sprintf("%s %s %s", colName, op, num)
		}
	}
	// No numeric condition found � try string value matching.
	return extractStringWhereCondition(input, tableName)
}

// parseNumericWord returns the string if it looks like a plain number, otherwise "".
func parseNumericWord(s string) string {
	s = strings.ReplaceAll(s, ",", "")
	if len(s) == 0 {
		return ""
	}
	hasDot := false
	for _, c := range s {
		if c == '.' {
			if hasDot {
				return ""
			}
			hasDot = true
		} else if c < '0' || c > '9' {
			return ""
		}
	}
	return s
}

// matchColumnInContext finds the column whose normalized name best matches the context string.
func matchColumnInContext(context string, cols []nlColEntry) string {
	// Exact normalized phrase match wins immediately
	for _, col := range cols {
		if strings.Contains(context, col.normalized) {
			return col.name
		}
	}
	// Partial word scoring
	bestCol := ""
	bestScore := 0
	for _, col := range cols {
		score := 0
		for _, part := range strings.Split(col.normalized, " ") {
			if len(part) > 3 && strings.Contains(context, part) {
				score++
			}
		}
		if score > bestScore {
			bestScore = score
			bestCol = col.name
		}
	}
	if bestScore > 0 {
		return bestCol
	}
	return ""
}

// sampleTableValues reads up to limit rows from a table's .dat file and returns
// a map of lowercase(value) ? {columnName, originalValue} for enum-like fields.
func sampleTableValues(tableName string, limit int) map[string]nlValueInfo {
	path := filepath.Join(db.GetDBPath(), tableName+".dat")
	f, err := os.Open(path)
	if err != nil {
		return nil
	}
	defer f.Close()

	result := make(map[string]nlValueInfo)
	scanner := bufio.NewScanner(f)
	count := 0
	for scanner.Scan() && count < limit {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var row map[string]interface{}
		if err := json.Unmarshal([]byte(line), &row); err != nil {
			continue
		}
		for col, val := range row {
			if val == nil {
				continue
			}
			s := fmt.Sprintf("%v", val)
			if len(s) == 0 || len(s) > 40 || parseNumericWord(s) != "" {
				continue // skip blanks, long strings, and numbers
			}
			lower := strings.ToLower(s)
			if _, exists := result[lower]; !exists {
				result[lower] = nlValueInfo{col: col, original: s}
			}
		}
		count++
	}
	return result
}

// extractStringWhereCondition builds a SQL WHERE clause for string/enum conditions
// like "not married", "is employed", "are female" using sampled data values.
func extractStringWhereCondition(input string, tableName string) string {
	valueMap := sampleTableValues(tableName, 40)
	if len(valueMap) == 0 {
		return ""
	}

	upper := strings.ToUpper(input)

	// Negation patterns: "NOT X", "ARE NOT X", etc.
	for _, pat := range []string{
		"WHO ARE NOT ", "WHO IS NOT ", "ARE NOT ", "IS NOT ", "NOT ", "NO ",
	} {
		idx := strings.Index(upper, pat)
		if idx == -1 {
			continue
		}
		remainder := strings.ToLower(strings.TrimSpace(input[idx+len(pat):]))
		remWords := strings.Fields(remainder)
		for n := min(3, len(remWords)); n >= 1; n-- {
			candidate := strings.Join(remWords[:n], " ")
			if info, ok := valueMap[candidate]; ok {
				return fmt.Sprintf("%s != '%s'", info.col, info.original)
			}
		}
	}

	// Affirmative patterns: "IS X", "ARE X", "WHO ARE X", etc.
	for _, pat := range []string{
		"WHO ARE ", "WHO IS ", "THAT ARE ", "THAT IS ", "ARE ", "IS ",
	} {
		idx := strings.Index(upper, pat)
		if idx == -1 {
			continue
		}
		remainder := strings.ToLower(strings.TrimSpace(input[idx+len(pat):]))
		remWords := strings.Fields(remainder)
		for n := min(3, len(remWords)); n >= 1; n-- {
			candidate := strings.Join(remWords[:n], " ")
			if info, ok := valueMap[candidate]; ok {
				return fmt.Sprintf("%s = '%s'", info.col, info.original)
			}
		}
	}

	// Last resort: any word in the query that matches a known enum value.
	for _, word := range strings.Fields(strings.ToLower(input)) {
		if len(word) < 4 {
			continue
		}
		if info, ok := valueMap[word]; ok {
			return fmt.Sprintf("%s = '%s'", info.col, info.original)
		}
	}
	return ""
}

func queryOllama(input string) (string, error) {
	context := buildDBContext()
	prompt := fmt.Sprintf(`You are a SQL expert. Convert natural language to SQL only.

%s

Rules:
1. Return ONLY the SQL query, nothing else
2. End with semicolon
3. Use exact table/column names from the schema

Natural language: %s
SQL:`, context, input)

	body, _ := json.Marshal(OllamaRequest{
		Model:  "llama3",
		Prompt: prompt,
		Stream: false,
		Options: map[string]interface{}{
			"temperature": 0.1,
			"num_predict": 100,
			"stop":        []string{"\n\n", "Explanation:", "Note:"},
		},
	})

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Post("http://localhost:11434/api/generate", "application/json", bytes.NewBuffer(body))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	var result OllamaResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", err
	}
	return result.Response, nil
}

func buildDBContext() string {
	var sb strings.Builder
	sb.WriteString("AVAILABLE TABLES:\n")
	for _, name := range db.GetAllTableNames() {
		if table, ok := db.GetTable(name); ok {
			var cols []string
			for _, c := range table.Columns {
				cols = append(cols, fmt.Sprintf("%s(%s)", c.Name, strings.ToLower(string(c.Type))))
			}
			sb.WriteString(fmt.Sprintf("- %s: %s\n", name, strings.Join(cols, ", ")))
		}
	}
	return sb.String()
}

func cleanSQL(response, original string) string {
	response = strings.TrimSpace(response)
	response = strings.TrimPrefix(response, "```sql")
	response = strings.TrimPrefix(response, "```")
	response = strings.TrimSuffix(response, "```")
	response = strings.TrimSpace(response)
	sqlKeywords := []string{"SELECT ", "INSERT ", "UPDATE ", "DELETE ", "CREATE ", "DROP ", "SHOW "}
	for _, line := range strings.Split(response, "\n") {
		line = strings.TrimSpace(line)
		upper := strings.ToUpper(line)
		for _, kw := range sqlKeywords {
			if strings.HasPrefix(upper, kw) {
				if !strings.HasSuffix(line, ";") {
					line += ";"
				}
				return line
			}
		}
	}
	if !strings.HasSuffix(response, ";") {
		response += ";"
	}
	return response
}

func writeJSON(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(data)
}
