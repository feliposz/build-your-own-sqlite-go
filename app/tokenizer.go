package main

import (
	"io"
	"log"
	"strings"
	"unicode"
)

type Tokenizer struct {
	Current int
	Source  string
	Tokens  []string
}

func NewTokenizer(source string) (tokenizer *Tokenizer) {
	tokenizer = &Tokenizer{}
	tokenizer.Source = source
	tokenizer.tokenize(source)
	return
}

func (t *Tokenizer) tokenize(source string) {
	r := strings.NewReader(source)
	tokens := []string{}
	for {
		ch, _, err := r.ReadRune()
		if err != nil {
			if err == io.EOF {
				break
			}
			panic(err)
		}
		if unicode.IsSpace(ch) {
			continue
		}

		switch ch {

		case '"':
			// quotes are not part of the token
			runes := []rune{}
			for {
				ch, _, err := r.ReadRune()
				if err != nil {
					if err == io.EOF {
						break
					}
					panic(err)
				}
				if ch == '"' {
					break
				}
				runes = append(runes, ch)
			}
			tokens = append(tokens, string(runes))

		case '[':
			// brackets are not part of the token
			runes := []rune{}
			for {
				ch, _, err := r.ReadRune()
				if err != nil {
					if err == io.EOF {
						break
					}
					panic(err)
				}
				if ch == ']' {
					break
				}
				runes = append(runes, ch)
			}
			tokens = append(tokens, string(runes))

		case '\'':
			// single-quote ARE part of the token
			runes := []rune{'\''}
			for {
				ch, _, err := r.ReadRune()
				if err != nil {
					if err == io.EOF {
						break
					}
					panic(err)
				}
				runes = append(runes, ch)
				if ch == '\'' {
					break
				}
			}
			tokens = append(tokens, string(runes))

		case '(', ')', ',', '*':
			tokens = append(tokens, string(ch))

		case '-', '+', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
			runes := []rune{ch}
		number_loop:
			for {
				ch, _, err := r.ReadRune()
				if err != nil {
					if err == io.EOF {
						break
					}
					panic(err)
				}
				switch ch {
				case '-', '+', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '.', 'e', 'E':
					runes = append(runes, ch)
				default:
					r.UnreadRune()
					break number_loop
				}
			}
			tokens = append(tokens, string(runes))

		default:
			runes := []rune{ch}
		default_loop:
			for {
				ch, _, err := r.ReadRune()
				if err != nil {
					if err == io.EOF {
						break
					}
					panic(err)
				}
				switch ch {
				case '(', ')', ',', '*', '[', '"':
					r.UnreadRune()
					break default_loop
				}
				if unicode.IsSpace(ch) {
					break
				}
				runes = append(runes, ch)
			}
			tokens = append(tokens, string(runes))
		}
	}
	t.Tokens = tokens
}

func (t *Tokenizer) AtEnd() bool {
	return t.Current >= len(t.Tokens)
}

func (t *Tokenizer) Match(s string) bool {
	if !t.AtEnd() && strings.EqualFold(t.Tokens[t.Current], s) {
		t.Current++
		return true
	}
	return false
}

func (t *Tokenizer) MustMatch(s string) {
	if !t.Match(s) {
		log.Printf("source: %q\n", t.Source)
		log.Fatal("syntax error - expected: ", s)
	}
}

func (t *Tokenizer) Peek() string {
	if !t.AtEnd() {
		return t.Tokens[t.Current]
	}
	return ""
}

func (t *Tokenizer) Advance() {
	if !t.AtEnd() {
		t.Current++
	}
}

func (t *Tokenizer) MustGetIdentifier() string {
	if t.AtEnd() {
		log.Fatal("syntax error - expected identifier")
	}
	result := t.Peek()
	t.Advance()
	return result
}
