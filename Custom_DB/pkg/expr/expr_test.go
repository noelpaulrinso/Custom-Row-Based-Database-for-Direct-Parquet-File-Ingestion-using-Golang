package expr

import (
	"testing"

	"Custom_DB/pkg/storage"
)

func TestSimpleComparison(t *testing.T) {
	row := storage.Row{"id": 1, "name": "Alice", "age": 20}
	e, err := ParseExpression("id = 1")
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	ok, err := e.Eval(row)
	if err != nil {
		t.Fatalf("eval err: %v", err)
	}
	if !ok {
		t.Fatalf("expected true")
	}
}

func TestLikeAndAndOr(t *testing.T) {
	row := storage.Row{"name": "Abel", "course": "AI/ML", "id": 2}
	e, err := ParseExpression("name LIKE 'A%' AND course = 'AI/ML'")
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	ok, err := e.Eval(row)
	if err != nil {
		t.Fatalf("eval err: %v", err)
	}
	if !ok {
		t.Fatalf("expected true")
	}
}

func TestInBetween(t *testing.T) {
	row := storage.Row{"score": 85}
	e, err := ParseExpression("score BETWEEN 50 AND 100")
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	ok, err := e.Eval(row)
	if err != nil {
		t.Fatalf("eval err: %v", err)
	}
	if !ok {
		t.Fatalf("expected true")
	}
}

func TestComplexOrNot(t *testing.T) {
	row := storage.Row{"a": 1, "b": 2}
	e, err := ParseExpression("NOT (a = 0 OR b = 3) AND (a < 5)")
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	ok, err := e.Eval(row)
	if err != nil {
		t.Fatalf("eval err: %v", err)
	}
	if !ok {
		t.Fatalf("expected true")
	}
}
