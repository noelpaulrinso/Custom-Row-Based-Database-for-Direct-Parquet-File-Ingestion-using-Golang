package parser

import (
	"fmt"
	"strings"
	"unicode"
)

// Command is a minimal parsed representation.
type Command struct {
	Type   string   // first keyword: SELECT, INSERT, UPDATE, DELETE, CREATE, SHOW, DROP, etc.
	Tokens []string // token stream
	Raw    string   // original input
}

// Tokenize splits SQL-like input into tokens, preserving quoted strings and separating punctuation.
func Tokenize(input string) []string {
	var tokens []string
	var cur strings.Builder
	inSingle := false
	inDouble := false

	flush := func() {
		if cur.Len() > 0 {
			tokens = append(tokens, cur.String())
			cur.Reset()
		}
	}

	for _, r := range input {
		switch {
		case r == '\'' && !inDouble:
			cur.WriteRune(r)
			inSingle = !inSingle
		case r == '"' && !inSingle:
			cur.WriteRune(r)
			inDouble = !inDouble
		case inSingle || inDouble:
			cur.WriteRune(r)
		case unicode.IsSpace(r):
			flush()
		case r == '(' || r == ')' || r == ',' || r == ';':
			flush()
			tokens = append(tokens, string(r))
		default:
			cur.WriteRune(r)
		}
	}
	flush()
	return tokens
}

// Parse creates a minimal Command from input.
func Parse(input string) (Command, error) {
	trim := strings.TrimSpace(input)
	if trim == "" {
		return Command{}, fmt.Errorf("empty input")
	}
	toks := Tokenize(trim)
	// remove trailing semicolon token(s)
	for len(toks) > 0 && toks[len(toks)-1] == ";" {
		toks = toks[:len(toks)-1]
	}
	if len(toks) == 0 {
		return Command{}, fmt.Errorf("empty input after trimming semicolons")
	}
	verb := strings.ToUpper(toks[0])
	return Command{
		Type:   verb,
		Tokens: toks,
		Raw:    trim,
	}, nil
}

// IndexOfKeyword finds a keyword (case-insensitive) in tokens and returns its index or -1.
func IndexOfKeyword(cmd Command, keyword string) int {
	ku := strings.ToUpper(keyword)
	for i, t := range cmd.Tokens {
		if strings.ToUpper(t) == ku {
			return i
		}
	}
	return -1
}
