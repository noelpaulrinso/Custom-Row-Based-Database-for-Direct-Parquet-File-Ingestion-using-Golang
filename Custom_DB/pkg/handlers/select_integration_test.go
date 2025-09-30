package handlers

import (
	"strings"
	"testing"

	"Custom_DB/pkg/parser"
	"Custom_DB/pkg/schema"
)

func TestHandleSelect_GroupBy_Count(t *testing.T) {
	dbPath := "../../data/my_first_db"
	db, err := schema.NewDatabase(dbPath)
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}

	cmd, err := parser.Parse("SELECT course, COUNT(*) AS cnt FROM students GROUP BY course;")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	out, err := HandleSelect(cmd, db)
	if err != nil {
		t.Fatalf("HandleSelect failed: %v", err)
	}
	if strings.TrimSpace(out) == "" {
		t.Fatalf("expected non-empty output for group by select; got empty")
	}
}

func TestHandleSelect_Distinct_Like(t *testing.T) {
	dbPath := "../../data/my_first_db"
	db, err := schema.NewDatabase(dbPath)
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}

	cmd, err := parser.Parse("SELECT DISTINCT course FROM students WHERE name LIKE 'A%';")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	out, err := HandleSelect(cmd, db)
	if err != nil {
		t.Fatalf("HandleSelect failed: %v", err)
	}
	// no assertion on contents, just ensure it ran without error and produced an output string
	if strings.TrimSpace(out) == "" {
		t.Fatalf("expected non-empty output for distinct/like select; got empty")
	}
}
