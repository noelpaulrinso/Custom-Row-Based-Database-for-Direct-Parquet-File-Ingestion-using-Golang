# CustomDB Web Frontend - Chat Interface

A modern, conversational chat interface for CustomDB - Custom Row-Based Database with Natural Language Support.

## 🚀 Features

- **💬 Chat Interface**: Interact with your database through a conversational chat window
- **Real-time Responses**: View query results in beautifully formatted chat messages
- **Database Explorer**: Browse all tables and their schemas in the sidebar
- **🤖 Natural Language Support**: Ask questions in plain English (powered by Ollama/Llama3)
- **📁 Import Dialog**: Easy-to-use import functionality for Parquet files and SQL commands
- **Quick Actions**: Fast access to common queries from the sidebar
- **Dark Theme**: Modern, easy-on-the-eyes dark interface
- **Responsive Design**: Works on desktop, tablet, and mobile devices

## 📋 Prerequisites

1. **Go**: Make sure Go is installed (1.16 or higher)
2. **Ollama** (Optional): For natural language query support
   - Install from: https://ollama.ai
   - Run: `ollama pull llama3`

## 🛠️ Getting Started

### 1. Start the Web Server

Navigate to your CustomDB directory and run:

```bash
go run server.go
```

You should see output like:
```
==============================================
   CustomDB Web Server
==============================================

Database loaded from: data/my_first_db

🚀 Server starting on http://localhost:8081
📊 Web interface available at http://localhost:8081
```

### 2. Open the Web Interface

Open your browser and go to:
```
http://localhost:8081
```

### 3. Start Chatting!

**Chat with your database:**
- Type any SQL query or natural language question
- Press Ctrl+Enter or click the send button
- See results instantly in the chat

**Example messages:**
```
SQL Mode:
- SELECT * FROM photos;
- INSERT INTO photos (id, name) VALUES (1, 'sunset');
- UPDATE photos SET name = 'beach' WHERE id = 1;
- SHOW TABLES;

Natural Language Mode (toggle the checkbox):
- show me all photos
- how many records in the photos table?
- find photo with id 1
- what tables do I have?
```

**Import Data:**
- Click the "📁 Import Data" button in the header
- Choose between:
  - Import from Parquet file (enter table name and file path)
  - Execute SQL commands (paste multiple SQL statements)

**View Database Info:**
- Click the "📊 Info" button to open the sidebar
- See all tables with their schemas
- Click on a table to quickly query it
- Use Quick Actions for common operations

## 📂 Project Structure

```
Custom_DB/
├── server.go              # Web server & REST API
├── main.go                # Original CLI application
├── web/
│   ├── index.html         # Main web interface
│   └── static/
│       ├── css/
│       │   └── style.css  # Styling
│       └── js/
│           └── app.js     # Frontend logic
├── pkg/
│   ├── handlers/          # Query handlers
│   ├── parser/            # SQL parser
│   ├── schema/            # Database schema
│   └── storage/           # Data storage
└── data/
    └── my_first_db/       # Database files
```

## 🔌 API Endpoints

The web server exposes the following REST API endpoints:

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/` | GET | Serves the web interface |
| `/api/query` | POST | Execute SQL or natural language query |
| `/api/tables` | GET | Get all tables and their schemas |
| `/api/natural-language` | POST | Convert natural language to SQL |
| `/api/image-dir` | GET/POST | Get or set image directory |

### Example API Usage
Send message/execute query
- **Click on table in sidebar**: Auto-fill SELECT query for that table
- **Click on Quick Action**: Send predefined query
- **Click example chip**: Send example query
curl -X POST http://localhost:8081/api/query \
  -H "Content-Type: application/json" \
  -d '{"query": "SELECT * FROM photos;", "isNatural": false}'
```

**Get all tables:**
```bash
curl http://localhost:8081/api/tables
```

## 🎨 Keyboard Shortcuts

- **Ctrl + Enter**: Execute query
- **Click on table**: Auto-fill SELECT query
- **Click on example**: Copy example to query editor

## 💡 Tips

1. **Chat History**: Scroll up to see previous queries and responses
3. **Table Quick Access**: Click tables in sidebar to instantly query them
4. **Import Multiple SQL**: Use the SQL import feature to execute multiple commands at once
5. **Responsive**: The chat interface adapts to your screen sizeolumns and types
4. **Results Export**: Copy results directly from the table display

## 🐛 Troubleshooting

**Server won't start:**
- Make sure port 8081 is not already in use
- Check that the database path exists: `data/my_first_db`

**Natural language not working:**
- Verify Ollama is running: `ollama list`
- Check if llama3 is installed: `ollama pull llama3`
- Make sure Ollama is accessible at `http://localhost:11434`

**Tables not showing:**
- Verify your database schema exists: `data/my_first_db/schema.json`
- Try the original CLI: `go run main.go` and run `SHOW TABLES;`

## 🔄 Switching Between CLI and Web Interface

You can use both interfaces with the same database:

**CLI Mode:**
```bash
go run main.go
```

**Web Mode:**
```bash
go run server.go
```

Both use the same database files in `data/my_first_db/`.

## 📝Start the server:**
   ```bash
   go run server.go
   ```

2. **Open browser and navigate to http://localhost:8082**

3. **Create a table:**
   - Click "Import Data" button
   - Paste in SQL Import section:
   ```sql
   CREATE TABLE students (id INT, name TEXT, course TEXT);
   ```
   - Click "⚡ Execute SQL"

4. **Insert data through chat:**
   - Type in chat: `INSERT INTO students (id, name, course) VALUES (1, 'John', 'CS');`
   - Send the message

5. **Query with SQL:**
   - Type: `SELECT * FROM students WHERE course = 'CS';`

6. **Query withOverview

### Chat Interface
- Conversational message-based interaction
- User messages appear on the right (blue)
- Bot responses appear on the left with formatted results
- Auto-scroll to latest message
- Timestamp on each message

### Import Dialog
- Import from Parquet files (enter table name and path)
- Execute multiple SQL commands at once
- Modal popup for easy access

### Sidebar
- Toggle on/off with Info button
- View all database tables
- Click tables to query them
- Quick action buttons for common queries
- Image directory settings

### Message Display
- Tables are rendered in a clean format within chat messages
- SQL queries are highlighted when generated from natural language
- Success/error indicators with color coding
- Row count display for table results

## 🌟 Coming Soon

- Query history saved locally
- Export chat history
- Copy individual messages
- Table visualization charts
- Syntax highlighting in SQL inputon
   - Browse tables in sidebar
   - Click on any table name to query it
   - "how many students are in the AI course?"
   - "find student named John"

## 🌟 Features Coming Soon

- Export results to CSV/JSON
- Query history
- Syntax highlighting in query editor
- Auto-complete for table/column names
- Dark/Light theme toggle

## 📄 License

Part of the CustomDB project - Custom Row-Based Database for Direct Parquet File Ingestion using Golang

---

**Enjoy using CustomDB Web Interface! 🚀**
