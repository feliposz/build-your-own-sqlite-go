package main

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
)

func TestPrintPageSize(t *testing.T) {
	db := NewDbContext("../sample.db")
	defer db.Close()
	result := new(bytes.Buffer)
	db.PrintDbInfo(result)
	expected := "database page size:  4096"
	if !strings.Contains(result.String(), expected) {
		t.Errorf("result does not contain string: %q", expected)
	}
}

func TestPrintNumberOfTables(t *testing.T) {
	db := NewDbContext("../sample.db")
	defer db.Close()
	result := new(bytes.Buffer)
	db.PrintDbInfo(result)
	expected := "number of tables:    3"
	if !strings.Contains(result.String(), expected) {
		t.Errorf("result does not contain string: %q", expected)
	}
}

func TestPrintTableNames(t *testing.T) {
	db := NewDbContext("../sample.db")
	defer db.Close()
	result := new(bytes.Buffer)
	db.PrintTables(result)
	tables := []string{"apples", "oranges"}
	for _, table := range tables {
		if !strings.Contains(result.String(), table) {
			t.Errorf("result does not contain table: %q", table)
		}
	}
}

func TestPrintIndexesNames(t *testing.T) {
	db := NewDbContext("../companies.db")
	defer db.Close()
	result := new(bytes.Buffer)
	db.PrintIndexes(result)
	indexes := []string{"idx_companies_country"}
	for _, index := range indexes {
		if !strings.Contains(result.String(), index) {
			t.Errorf("result does not contain index: %q", index)
		}
	}
}

func TestPrintSchema(t *testing.T) {
	db := NewDbContext("../sample.db")
	defer db.Close()
	result := new(bytes.Buffer)
	db.PrintSchema(result)
	expected := []string{"CREATE TABLE apples", "CREATE TABLE oranges", "name text,", "id integer primary key autoincrement,"}
	for _, text := range expected {
		if !strings.Contains(result.String(), text) {
			t.Errorf("result does not contain text: %q", text)
		}
	}
}

func TestCountRows(t *testing.T) {
	db := NewDbContext("../sample.db")
	defer db.Close()

	tests := []struct{ query, expected string }{
		{"select count(*) from apples", "4"},
		{"select count(*) from oranges", "6"},
	}

	for _, test := range tests {
		result := new(bytes.Buffer)
		db.HandleSelect(test.query, result)
		if !strings.HasPrefix(result.String(), test.expected) {
			t.Errorf("result does not contain text: %q", test.expected)
		}
	}
}

func TestSelectSingleColumn(t *testing.T) {
	db := NewDbContext("../sample.db")
	defer db.Close()

	tests := []struct{ query, expected string }{
		{"select name from apples", "Honeycrisp"},
		{"select description from oranges", "usually seedless, great for snacking"},
	}

	for _, test := range tests {
		result := new(bytes.Buffer)
		db.HandleSelect(test.query, result)
		if !strings.Contains(result.String(), test.expected) {
			fmt.Print(result.String())
			t.Errorf("result does not contain text: %q", test.expected)
		}
	}
}

func TestSelectMultipleColumns(t *testing.T) {
	db := NewDbContext("../sample.db")
	defer db.Close()

	tests := []struct{ query, expected string }{
		{"select name, color from apples", "Golden Delicious|Yellow"},
		{"select name,description from oranges", "Valencia Orange|best for juicing"},
	}

	for _, test := range tests {
		result := new(bytes.Buffer)
		db.HandleSelect(test.query, result)
		if !strings.Contains(result.String(), test.expected) {
			fmt.Print(result.String())
			t.Errorf("result does not contain text: %q", test.expected)
		}
	}
}

func TestFilterDataWithAWhereClause(t *testing.T) {
	db := NewDbContext("../sample.db")
	defer db.Close()

	tests := []struct{ query, mustContain, mustNotContain string }{
		{"select color from apples where name = 'Fuji'", "Red", "Yellow"},
		{"select name from oranges where description = 'sweet and tart'", "Tangelo", "Clementine"},
	}

	for _, test := range tests {
		result := new(bytes.Buffer)
		db.HandleSelect(test.query, result)
		if !strings.Contains(result.String(), test.mustContain) {
			t.Errorf("result does not contain text: %q", test.mustContain)
		}
		if strings.Contains(result.String(), test.mustNotContain) {
			t.Errorf("result must not contain text: %q", test.mustNotContain)
		}
	}
}

func TestRetrieveDataUsingAnIndex(t *testing.T) {
	db := NewDbContext("../companies.db")
	defer db.Close()

	tests := []struct{ query, mustContain, mustNotContain string }{
		{"SELECT id, name FROM companies WHERE country = 'micronesia'", "6387751|fsm development bank", "986681|isn network company limited"},
		{"SELECT id, name FROM companies WHERE country = 'north korea'", "2828420|beacon point ltd", "3696903|nanofabrica"},
		{"SELECT id, name FROM companies WHERE country = 'tonga'", "3583436|leiola group limited", "6634629|asmara rental"},
		{"SELECT id, name FROM companies WHERE country = 'eritrea'", "121311|unilink s.c.", "3485462|pyongyang university of science & technology (pust)"},
		{"SELECT id, name FROM companies WHERE country = 'republic of the congo'", "2995059|petroleum trading congo e&p sa", "2466228|web tchad"},
		{"SELECT id, name FROM companies WHERE country = 'montserrat'", "288999|government of montserrat", "1573653|initial innovation limited"},
		{"SELECT id, name FROM companies WHERE country = 'chad'", "6828605|tigo tchad", "5316703|the abella group llc"},
	}

	for _, test := range tests {
		result := new(bytes.Buffer)
		db.HandleSelect(test.query, result)
		if !strings.Contains(result.String(), test.mustContain) {
			t.Errorf("result does not contain text: %q", test.mustContain)
		}
		if strings.Contains(result.String(), test.mustNotContain) {
			t.Errorf("result must not contain text: %q", test.mustNotContain)
		}
	}
}
