package main

import (
	"slices"
	"testing"
)

func TestTokenizer(t *testing.T) {
	tests := []struct {
		source   string
		expected []string
	}{
		{"abc def ghi", []string{"abc", "def", "ghi"}},
		{",abc, def,ghi   ,  jkl  , mno,", []string{",", "abc", ",", "def", ",", "ghi", ",", "jkl", ",", "mno", ","}},
		{"123 456 789 3.1415926 -123 +45.12 +1e10 -3.5e-1", []string{"123", "456", "789", "3.1415926", "-123", "+45.12", "+1e10", "-3.5e-1"}},
		{"\"abc\",[def],'ghi'", []string{"abc", ",", "def", ",", "'ghi'"}},
		{"abc(((*,*)))def", []string{"abc", "(", "(", "(", "*", ",", "*", ")", ")", ")", "def"}},
	}

	for _, test := range tests {
		tokenizer := NewTokenizer(test.source)
		if slices.Compare(test.expected, tokenizer.Tokens) != 0 {
			t.Errorf("expected: %q - got: %q\n", test.expected, tokenizer.Tokens)
		}
	}
}

func TestParseCreateIndex(t *testing.T) {
	indexName, tableName, columns := parseCreateIndex("create index idx on tab (a, b desc, c asc)")
	if indexName != "idx" {
		t.Errorf("expected index name: %q - got: %q\n", "idx", indexName)
	}
	if tableName != "tab" {
		t.Errorf("expected table name: %q - got: %q\n", "tab", tableName)
	}
	for i, name := range []string{"a", "b", "c"} {
		if columns[i].Name != name {
			t.Errorf("expected column name: %q - got: %q\n", name, columns[i].Name)
		}
	}
	for i, order := range []string{"ASC", "DESC", "ASC"} {
		if columns[i].Type != order {
			t.Errorf("expected column order: %q - got: %q\n", order, columns[i].Type)
		}
	}
}
