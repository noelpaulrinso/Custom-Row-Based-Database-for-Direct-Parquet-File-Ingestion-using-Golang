package parser

import "testing"

func TestParse_EmptyInput(t *testing.T) {
	if _, err := Parse(""); err == nil {
		t.Fatalf("expected error for empty input")
	}
}

func TestTokenize_OnlyPunctuation(t *testing.T) {
	toks := Tokenize(";;; ,,,")
	if len(toks) == 0 {
		t.Fatalf("expected tokens for punctuation, got none")
	}
}
