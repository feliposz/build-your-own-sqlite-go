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
