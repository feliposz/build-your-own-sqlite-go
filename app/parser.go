package main

import (
	"fmt"
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

func parseColumns(sql string) (columns []ColumnDef, constraints []string) {
	if sql == "" {
		return
	}
	if !strings.HasPrefix(sql, "CREATE") {
		log.Fatal("invalid DDL statement")
	}
	leftParen := strings.Index(sql, "(")
	rightParen := strings.LastIndex(sql, ")")
	if leftParen < 0 || rightParen < 0 {
		return
	}
	// remove everything before first "(" and after last ")"
	sql = sql[leftParen+1 : rightParen]

	// tokenize column definitions for processing
	tokenizer := NewTokenizer(sql)
	tokens := tokenizer.Tokens

	if debugMode {
		fmt.Printf("tokens: %#v\n", tokens)
		fmt.Println()
	}

	for i := 0; i < len(tokens); i++ {
		switch strings.ToUpper(tokens[i]) {
		case "PRIMARY", "CONSTRAINT", "UNIQUE", "CHECK", "FOREIGN":
			// table constraints start with one of these keywords and everything is part of the definition until the next ","
			for i < len(tokens) && tokens[i] != "," {
				constraints = append(constraints, tokens[i])
				i++
			}
		default:
			// column definitions always start with column name
			column := ColumnDef{}
			column.Name = tokens[i]
			i++
			// type and constraints are optional
			typeTokens := []string{}
			for i < len(tokens) && tokens[i] != "," {
				switch strings.ToUpper(tokens[i]) {
				case "PRIMARY", "CONSTRAINT", "UNIQUE", "CHECK", "REFERENCES", "NOT", "NULL", "DEFAULT", "COLLATE", "GENERATED":
					// column constraints start with one of these keywords and everything else until the "," is constraints
					for i < len(tokens) && tokens[i] != "," {
						column.Constraints = append(column.Constraints, tokens[i])
						i++
					}
				default:
					// everything after the column name and before the constraints are part of the "type name"
					typeTokens = append(typeTokens, tokens[i])
					i++
				}
			}
			column.Type = strings.Join(typeTokens, " ")
			if debugMode {
				fmt.Printf("column: %#v\n", column)
			}
			columns = append(columns, column)
		}
	}

	// if primary key is defined on table level, add it to the proper column
	// TODO: handle multi-column PKs
	for i := 0; i < len(constraints); i++ {
		if strings.ToUpper(constraints[i]) == "PRIMARY" && strings.ToUpper(constraints[i+1]) == "KEY" {
			columnName := strings.Trim(constraints[i+2], "()")
			for j := range columns {
				if columns[j].Name == columnName {
					columns[j].Constraints = append(columns[j].Constraints, "PRIMARY", "KEY")
				}
			}
			i += 2
		}
	}

	if debugMode {
		fmt.Println()
		fmt.Printf("constraints: %#v\n", constraints)
		fmt.Println("-----")
	}
	return
}

func parseCreateIndex(sql string) (indexName, tableName string, columns []ColumnDef) {
	t := NewTokenizer(sql)
	if t.AtEnd() {
		return
	}
	t.MustMatch("CREATE")
	t.Match("UNIQUE")
	t.MustMatch("INDEX")
	if t.Match("IF") {
		t.MustMatch("NOT")
		t.MustMatch("EXISTS")
	}
	indexName = t.MustGetIdentifier()
	t.MustMatch("ON")
	tableName = t.MustGetIdentifier()
	t.MustMatch("(")
	for {
		column := ColumnDef{}
		column.Name = t.MustGetIdentifier()
		if t.Match("COLLATE") {
			collationName := t.MustGetIdentifier()
			column.Constraints = append(column.Constraints, "COLLATE", collationName)
		}
		if t.Match("DESC") {
			column.Type = "DESC"
		} else {
			column.Type = "ASC"
			t.Match("ASC")
		}
		columns = append(columns, column)
		if !t.Match(",") {
			t.MustMatch(")")
			break
		}
	}
	return
}
