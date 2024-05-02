package main

import (
	"fmt"
	"strconv"
	"strings"
)

func parseCreateTable(sql string) (tableName string, columns []ColumnDef, constraints []string, err error) {
	t := NewTokenizer(sql)
	if t.AtEnd() {
		return
	}
	if debugMode {
		fmt.Printf("tokens: %#v\n", t.Tokens)
		fmt.Println()
	}
	err = t.MustMatch("CREATE")
	if err != nil {
		return
	}
	if t.Match("TEMP") || t.Match("TEMPORARY") {
		constraints = append(constraints, t.Previous())
	}
	err = t.MustMatch("TABLE")
	if err != nil {
		return
	}
	if t.Match("IF") {
		err = t.MustMatch("NOT")
		if err != nil {
			return
		}
		err = t.MustMatch("EXISTS")
		if err != nil {
			return
		}
	}
	tableName, err = t.MustGetIdentifier()
	if err != nil {
		return
	}
	err = t.MustMatch("(")
	if err != nil {
		return
	}
	for {
		constraint := []string{}
		if t.Match("CONSTRAINT") {
			var constraintName string
			constraintName, err = t.MustGetIdentifier()
			if err != nil {
				return
			}
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
			err = fmt.Errorf("invalid constraint: %s", t.Peek())
			return
		} else {
			column := ColumnDef{}
			column.Name, err = t.MustGetIdentifier()
			if err != nil {
				return
			}
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
					var typeToken string
					typeToken, err = t.MustGetIdentifier()
					if err != nil {
						return
					}
					typeTokens = append(typeTokens, typeToken)
				}
			}
			column.Type = strings.Join(typeTokens, " ")
			if debugMode {
				fmt.Printf("column: %#v\n", column)
			}
			columns = append(columns, column)
		}
		if !t.Match(",") {
			err = t.MustMatch(")")
			if err != nil {
				return
			}
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

func parseCreateIndex(sql string) (indexName, tableName string, columns []ColumnDef, err error) {
	t := NewTokenizer(sql)
	if t.AtEnd() {
		return
	}
	err = t.MustMatch("CREATE")
	if err != nil {
		return
	}
	t.Match("UNIQUE")
	err = t.MustMatch("INDEX")
	if err != nil {
		return
	}
	if t.Match("IF") {
		err = t.MustMatch("NOT")
		if err != nil {
			return
		}
		err = t.MustMatch("EXISTS")
		if err != nil {
			return
		}
	}
	indexName, err = t.MustGetIdentifier()
	if err != nil {
		return
	}
	err = t.MustMatch("ON")
	if err != nil {
		return
	}
	tableName, err = t.MustGetIdentifier()
	if err != nil {
		return
	}
	err = t.MustMatch("(")
	if err != nil {
		return
	}
	for {
		column := ColumnDef{}
		column.Name, err = t.MustGetIdentifier()
		if err != nil {
			return
		}
		if t.Match("COLLATE") {
			var collationName string
			collationName, err = t.MustGetIdentifier()
			if err != nil {
				return
			}
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
			err = t.MustMatch(")")
			if err != nil {
				return
			}
			break
		}
	}
	return
}

func parseSelectStatement(sql string) (tableName string, columns []string, filterColumnName string, filterValue any, err error) {
	t := NewTokenizer(sql)
	err = t.MustMatch("SELECT")
	if err != nil {
		return
	}
	for {
		if t.Match("COUNT") {
			err = t.MustMatch("(")
			if err != nil {
				return
			}
			err = t.MustMatch("*")
			if err != nil {
				return
			}
			err = t.MustMatch(")")
			if err != nil {
				return
			}
			columns = append(columns, "COUNT(*)")
		} else {
			var column string
			column, err = t.MustGetIdentifier()
			if err != nil {
				return
			}
			columns = append(columns, column)
		}
		if !t.Match(",") {
			break
		}
	}
	err = t.MustMatch("FROM")
	if err != nil {
		return
	}
	tableName, err = t.MustGetIdentifier()
	if err != nil {
		return
	}
	if t.Match("WHERE") {
		filterColumnName, err = t.MustGetIdentifier()
		if err != nil {
			return
		}
		t.MustMatch("=")
		var value string
		value, err = t.MustGetIdentifier()
		if err != nil {
			return
		}
		if value[0] == '\'' {
			filterValue = strings.Trim(value, "'")
		} else {
			filterValue, err = strconv.ParseInt(value, 10, 64)
			if err != nil {
				if _, isNumError := err.(*strconv.NumError); isNumError {
					filterValue, err = strconv.ParseFloat(value, 64)
				}
				if err != nil {
					return
				}
			}
		}
	}
	return
}
