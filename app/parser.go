package main

import (
	"fmt"
	"log"
	"strconv"
	"strings"
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

func parseSelectStatement(sql string) (tableName string, columns []string, filterColumnName string, filterValue any) {
	t := NewTokenizer(sql)
	t.MustMatch("SELECT")
	for {
		if t.Match("COUNT") {
			t.MustMatch("(")
			t.MustMatch("*")
			t.MustMatch(")")
			columns = append(columns, "COUNT(*)")
		} else {
			column := t.MustGetIdentifier()
			columns = append(columns, column)
		}
		if !t.Match(",") {
			break
		}
	}
	t.MustMatch("FROM")
	tableName = t.MustGetIdentifier()
	if t.Match("WHERE") {
		filterColumnName = t.MustGetIdentifier()
		t.MustMatch("=")
		value := t.MustGetIdentifier()
		if value[0] == '\'' {
			filterValue = strings.Trim(value, "'")
		} else {
			var err error
			filterValue, err = strconv.ParseInt(value, 10, 64)
			if err != nil {
				if _, isNumError := err.(*strconv.NumError); isNumError {
					filterValue, err = strconv.ParseFloat(value, 64)
				}
				if err != nil {
					log.Fatalf("error converting value %q: %v", value, err)
				}
			}
		}
	}
	return
}
