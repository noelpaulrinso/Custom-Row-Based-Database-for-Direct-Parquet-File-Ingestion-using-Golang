package handlers

import (
	"strings"
	"testing"

	"Custom_DB/pkg/parser"
	"Custom_DB/pkg/schema"
	"Custom_DB/pkg/storage"
)

func TestHandleSelect_MissingFromReturnsError(t *testing.T) {
	d := t.TempDir()
	db, err := schema.NewDatabase(d)
	if err != nil {
		t.Fatalf("new db: %v", err)
	}
	// create simple table
	tbl := schema.Table{Name: "u", Columns: []schema.Column{{Name: "id", Type: schema.Integer}}}
	if err := db.AddTable(tbl); err != nil {
		t.Fatalf("add table: %v", err)
	}

	cmd, err := parser.Parse("SELECT id u;") // missing FROM
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	_, err = HandleSelect(cmd, db)
	if err == nil {
		t.Fatalf("expected error for SELECT missing FROM, got nil")
	}
}

func TestHandleSelect_GroupByWithoutCount_IsLenient(t *testing.T) {
	d := t.TempDir()
	db, err := schema.NewDatabase(d)
	if err != nil {
		t.Fatalf("new db: %v", err)
	}
	// students-like table
	tbl := schema.Table{Name: "students", Columns: []schema.Column{{Name: "name", Type: schema.Text}, {Name: "course", Type: schema.Text}}}
	if err := db.AddTable(tbl); err != nil {
		t.Fatalf("add table: %v", err)
	}
	f, err := storage.NewTableFile(db.GetDBPath(), "students")
	if err != nil {
		t.Fatalf("new table file: %v", err)
	}
	_ = f.AppendRow(storage.Row{"name": "A", "course": "CS"})
	_ = f.AppendRow(storage.Row{"name": "B", "course": "CS"})
	_ = f.AppendRow(storage.Row{"name": "C", "course": "Math"})

	cmd, err := parser.Parse("SELECT course FROM students GROUP BY course;")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	out, err := HandleSelect(cmd, db)
	if err != nil {
		t.Fatalf("expected no error for lenient GROUP BY, got: %v", err)
	}
	// although COUNT wasn't requested, current implementation returns grouped counts.
	if !strings.Contains(strings.ToLower(out), "count") {
		t.Fatalf("expected output to include 'count' column for grouped results, got: %s", out)
	}
	if !strings.Contains(out, "CS") || !strings.Contains(out, "Math") {
		t.Fatalf("expected grouped keys in output, got: %s", out)
	}
}
