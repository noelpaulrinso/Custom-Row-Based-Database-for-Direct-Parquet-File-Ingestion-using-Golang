package parser

import (
	"reflect"
	"testing"
)

func TestTokenizeBasic(t *testing.T) {
	in := "SELECT name, age FROM users WHERE name LIKE 'A%';"
	exp := []string{"SELECT", "name", ",", "age", "FROM", "users", "WHERE", "name", "LIKE", "'A%'", ";"}
	got := Tokenize(in)
	if !reflect.DeepEqual(got, exp) {
		t.Fatalf("Tokenize mismatch.\nGot:  %#v\nWant: %#v", got, exp)
	}
}

func TestParseVerb(t *testing.T) {
	cmd, err := Parse("  select * from users; ")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	if cmd.Type != "SELECT" {
		t.Fatalf("Unexpected verb: %s", cmd.Type)
	}
}
