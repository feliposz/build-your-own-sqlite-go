package main

import (
	"testing"
)

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

func TestParseSelectStatement(t *testing.T) {
	tableName, columnNames, filterColumn, filterValue := parseSelectStatement("select a, b, c, *, count(*) from tab where x = '123'")
	if tableName != "tab" {
		t.Errorf("expected table name: %q - got: %q\n", "tab", tableName)
	}
	for i, name := range []string{"a", "b", "c", "*", "COUNT(*)"} {
		if columnNames[i] != name {
			t.Errorf("expected column name: %q - got: %q\n", name, columnNames[i])
		}
	}
	if filterColumn != "x" {
		t.Errorf("expected filter column name: %q - got: %q\n", "x", filterColumn)
	}
	if compareAny("123", filterValue) != 0 {
		t.Errorf("expected filter value: %q - got: %q\n", "123", filterValue)
	}
}
