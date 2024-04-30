package main

import (
	"fmt"
	"log"
	"strconv"
	"strings"
)

func parseCreateTable(sql string) (tableName string, columns []ColumnDef, constraints []string) {
	t := NewTokenizer(sql)
	if t.AtEnd() {
		return
	}
	if debugMode {
		fmt.Printf("tokens: %#v\n", t.Tokens)
		fmt.Println()
	}
	t.MustMatch("CREATE")
	if t.Match("TEMP") || t.Match("TEMPORARY") {
		constraints = append(constraints, t.Previous())
	}
	t.MustMatch("TABLE")
	if t.Match("IF") {
		t.MustMatch("NOT")
		t.MustMatch("EXISTS")
	}
	tableName = t.MustGetIdentifier()
	t.MustMatch("(")
	for {
		constraint := []string{}
		if t.Match("CONSTRAINT") {
			constraintName := t.MustGetIdentifier()
			constraint = append(constraint, "CONSTRAINT", constraintName)
		}
		if t.Match("PRIMARY") || t.Match("UNIQUE") || t.Match("CHECK") || t.Match("FOREIGN") {
			// TODO: parse syntax for each constraint type
			constraint = append(constraint, t.Previous())
			for !t.AtEnd() {
				token := t.Peek()
				if token == "," || token == ")" {
					break
				}
				constraint = append(constraints, token)
				t.Advance()
			}
			constraints = append(constraints, strings.Join(constraint, " "))
		} else if len(constraint) > 0 {
			log.Fatal("invalid constraint")
		} else {
			column := ColumnDef{}
			column.Name = t.MustGetIdentifier()
			typeTokens := []string{}
			for !t.AtEnd() {
				token := t.Peek()
				if token == "," || token == ")" {
					break
				}
				if t.Match("PRIMARY") || t.Match("CONSTRAINT") || t.Match("UNIQUE") || t.Match("CHECK") || t.Match("REFERENCES") || t.Match("NOT") || t.Match("NULL") || t.Match("DEFAULT") || t.Match("COLLATE") || t.Match("GENERATED") {
					constraint = append(constraints, t.Previous())
					// TODO: parse syntax for each constraint type
					for !t.AtEnd() {
						token := t.Peek()
						if token == "," || token == ")" {
							break
						}
						constraint = append(constraint, token)
						t.Advance()
					}
					column.Constraints = append(column.Constraints, strings.Join(constraint, " "))
				} else {
					typeTokens = append(typeTokens, t.MustGetIdentifier())
				}
			}
			column.Type = strings.Join(typeTokens, " ")
			if debugMode {
				fmt.Printf("column: %#v\n", column)
			}
			columns = append(columns, column)
		}
		if !t.Match(",") {
			t.MustMatch(")")
			break
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
