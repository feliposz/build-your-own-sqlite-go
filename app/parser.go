package main

import (
	"fmt"
	"io"
	"log"
	"strings"
	"unicode"
)

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
	r := strings.NewReader(sql)
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

		if ch == '"' {
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
		} else if ch == '[' {
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
		} else if ch == '(' {
			// parenthesis ARE part of the token
			runes := []rune{'('}
			for {
				ch, _, err := r.ReadRune()
				if err != nil {
					if err == io.EOF {
						break
					}
					panic(err)
				}
				runes = append(runes, ch)
				if ch == ')' {
					break
				}
			}
			tokens = append(tokens, string(runes))
		} else if ch == ',' {
			tokens = append(tokens, ",")
		} else {
			runes := []rune{ch}
			for {
				ch, _, err := r.ReadRune()
				if err != nil {
					if err == io.EOF {
						break
					}
					panic(err)
				}
				if ch == ',' {
					r.UnreadRune()
					break
				}
				if unicode.IsSpace(ch) {
					break
				}
				runes = append(runes, ch)
			}
			tokens = append(tokens, string(runes))
		}
	}

	if debugMode {
		fmt.Printf("%#v\n", tokens)
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
				fmt.Printf("%#v\n", column)
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

	return
}
