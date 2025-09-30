package handlers

import (
	"strings"
	"testing"

	"Custom_DB/pkg/parser"
	"Custom_DB/pkg/schema"
	"Custom_DB/pkg/storage"
)

func TestHandleSelect_HappyPathProjection(t *testing.T) {
	d := t.TempDir()
	db, err := schema.NewDatabase(d)
	if err != nil {
		t.Fatalf("new db: %v", err)
	}

	tbl := schema.Table{Name: "users", Columns: []schema.Column{{Name: "id", Type: schema.Integer}, {Name: "name", Type: schema.Text}}}
	if err := db.AddTable(tbl); err != nil {
		t.Fatalf("add table: %v", err)
	}

	tf, err := storage.NewTableFile(db.GetDBPath(), "users")
	if err != nil {
		t.Fatalf("new table file: %v", err)
	}
	if err := tf.AppendRow(storage.Row{"id": 1, "name": "Alice"}); err != nil {
		t.Fatalf("append row: %v", err)
	}
	if err := tf.AppendRow(storage.Row{"id": 2, "name": "Bob"}); err != nil {
		t.Fatalf("append row: %v", err)
	}

	cmd, err := parser.Parse("SELECT name FROM users;")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	out, err := HandleSelect(cmd, db)
	if err != nil {
		t.Fatalf("handle select err: %v", err)
	}
	if !strings.Contains(out, "name") {
		t.Fatalf("expected header to contain 'name', got: %s", out)
	}
	if !strings.Contains(out, "Alice") || !strings.Contains(out, "Bob") {
		t.Fatalf("expected rows with Alice and Bob, got: %s", out)
	}
}

func TestHandleSelect_EmptyTable(t *testing.T) {
	d := t.TempDir()
	db, err := schema.NewDatabase(d)
	if err != nil {
		t.Fatalf("new db: %v", err)
	}
	tbl := schema.Table{Name: "emptytab", Columns: []schema.Column{{Name: "col1", Type: schema.Text}, {Name: "col2", Type: schema.Integer}}}
	if err := db.AddTable(tbl); err != nil {
		t.Fatalf("add table: %v", err)
	}

	cmd, err := parser.Parse("SELECT * FROM emptytab;")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	out, err := HandleSelect(cmd, db)
	if err != nil {
		t.Fatalf("handle select err: %v", err)
	}
	if !strings.Contains(out, "col1") || !strings.Contains(out, "col2") {
		t.Fatalf("expected header to contain columns for empty table; got: %s", out)
	}
	// trim and count lines: header + separator expected, no data rows
	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) < 2 {
		t.Fatalf("expected at least header and separator lines, got: %v", lines)
	}
	// ensure no data rows beyond header/separator
	if len(lines) > 2 {
		// allow possibility of blank trailing line; check that additional lines are not non-empty data
		for _, l := range lines[2:] {
			if strings.TrimSpace(l) != "" {
				t.Fatalf("expected no data rows for empty table, found: %s", l)
			}
		}
	}
}

func TestHandleSelect_MissingProjectionColumn(t *testing.T) {
	d := t.TempDir()
	db, err := schema.NewDatabase(d)
	if err != nil {
		t.Fatalf("new db: %v", err)
	}
	tbl := schema.Table{Name: "t1", Columns: []schema.Column{{Name: "a", Type: schema.Text}}}
	if err := db.AddTable(tbl); err != nil {
		t.Fatalf("add table: %v", err)
	}
	f, err := storage.NewTableFile(db.GetDBPath(), "t1")
	if err != nil {
		t.Fatalf("new table file: %v", err)
	}
	if err := f.AppendRow(storage.Row{"a": "val"}); err != nil {
		t.Fatalf("append row: %v", err)
	}

	cmd, err := parser.Parse("SELECT missing FROM t1;")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	out, err := HandleSelect(cmd, db)
	if err != nil {
		t.Fatalf("expected no error when projecting missing column; got: %v", err)
	}
	// missing column should produce NULL in output
	if !strings.Contains(out, "NULL") {
		t.Fatalf("expected NULL placeholder for missing column, got: %s", out)
	}
}

func TestHandleSelect_WhereMissingColumnError(t *testing.T) {
	d := t.TempDir()
	db, err := schema.NewDatabase(d)
	if err != nil {
		t.Fatalf("new db: %v", err)
	}
	tbl := schema.Table{Name: "people", Columns: []schema.Column{{Name: "name", Type: schema.Text}}}
	if err := db.AddTable(tbl); err != nil {
		t.Fatalf("add table: %v", err)
	}

	cmd, err := parser.Parse("SELECT name FROM people WHERE age = 30;")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	_, err = HandleSelect(cmd, db)
	if err == nil {
		t.Fatalf("expected error when WHERE references missing column 'age', got nil")
	}
}
