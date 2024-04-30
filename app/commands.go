package main

import (
	"fmt"
	"io"
	"log"
	"slices"
	"strings"
)

func (db *DbContext) PrintDbInfo(writer io.Writer) {
	info := db.Info
	encodingDescription := "?"
	switch info.TextEncoding {
	case 1:
		encodingDescription = " (utf8)"
	case 2:
		encodingDescription = " (utf16le)"
	case 3:
		encodingDescription = " (utf16be)"
	}
	fmt.Fprintf(writer, "database page size:  %d\n", info.DatabasePageSize)
	fmt.Fprintf(writer, "write format:        %d\n", info.WriteFormat)
	fmt.Fprintf(writer, "read format:         %d\n", info.ReadFormat)
	fmt.Fprintf(writer, "reserved bytes:      %d\n", info.ReservedBytes)
	fmt.Fprintf(writer, "file change counter: %d\n", info.FileChangeCounter)
	fmt.Fprintf(writer, "database page count: %d\n", info.DatabasePageCount)
	fmt.Fprintf(writer, "freelist page count: %d\n", info.FreelistPageCount)
	fmt.Fprintf(writer, "schema cookie:       %d\n", info.SchemaCookie)
	fmt.Fprintf(writer, "schema format:       %d\n", info.SchemaFormat)
	fmt.Fprintf(writer, "default cache size:  %d\n", info.DefaultCacheSize)
	fmt.Fprintf(writer, "autovacuum top root: %d\n", info.AutovacuumTopRoot)
	fmt.Fprintf(writer, "incremental vacuum:  %d\n", info.IncrementalVacuum)
	fmt.Fprintf(writer, "text encoding:       %d%s\n", info.TextEncoding, encodingDescription)
	fmt.Fprintf(writer, "user version:        %d\n", info.UserVersion)
	fmt.Fprintf(writer, "application id:      %d\n", info.ApplicationID)
	fmt.Fprintf(writer, "software version:    %d\n", info.SoftwareVersion)
	fmt.Fprintf(writer, "number of tables:    %d\n", info.NumberOfTables)
	fmt.Fprintf(writer, "number of indexes:   %d\n", info.NumberOfIndexes)
	fmt.Fprintf(writer, "number of triggers:  %d\n", info.NumberOfTriggers)
	fmt.Fprintf(writer, "number of views:     %d\n", info.NumberOfViews)
	fmt.Fprintf(writer, "schema size:         %d\n", info.SchemaSize)
}

func (db *DbContext) PrintTables(writer io.Writer) {
	tables := []string{}
	for _, entry := range db.Schema {
		if (entry.Type == "table" || entry.Type == "view") && !strings.HasPrefix(entry.Name, "sqlite_") {
			tables = append(tables, entry.Name)
		}
	}
	slices.Sort(tables)
	fmt.Fprintln(writer, strings.Join(tables, " "))
}

func (db *DbContext) PrintIndexes(writer io.Writer) {
	for _, entry := range db.Schema {
		if entry.Type == "index" {
			fmt.Fprint(writer, entry.Name, " ")
		}
	}
	fmt.Fprintln(writer)
}

func (db *DbContext) PrintSchema(writer io.Writer) {
	for _, entry := range db.Schema {
		if entry.SQL != "" {
			fmt.Fprintf(writer, "%s;\n", entry.SQL)
		}
	}
}

func (db *DbContext) HandleSelect(query string, writer io.Writer) {

	queryTableName, queryColumnNames, filterColumnName, filterValue := parseSelectStatement(query)

	rootPage := 0
	var tableColumns []ColumnDef

	if strings.EqualFold(queryTableName, "sqlite_schema") || strings.EqualFold(queryTableName, "sqlite_master") {
		rootPage = 1
		// sqlite_schema has no table definition - this is the one from the docs: https://www.sqlite.org/fileformat.html#storage_of_the_sql_database_schema
		_, tableColumns, _ = parseCreateTable("CREATE TABLE sqlite_schema(type text, name text, tbl_name text, rootpage integer, sql text);")
	}

	for _, entry := range db.Schema {
		if entry.Type == "table" && strings.EqualFold(queryTableName, entry.Name) {
			rootPage = entry.RootPage
			tableColumns = entry.Columns
			if debugMode {
				fmt.Printf("select table entry: %#v\n", entry)
			}
			break
		}
	}

	if rootPage == 0 {
		log.Fatal("no such table:", queryTableName)
	}

	queryColumnNumbers := []int{}

	countingOnly := strings.EqualFold(queryColumnNames[0], "COUNT(*)")

	// use a fast count if no filter is used to avoid processing all data
	if countingOnly {
		if filterColumnName == "" {
			rowCount := db.fastCountRows(rootPage)
			fmt.Fprintln(writer, rowCount)
			return
		}
	} else {
		// replace "*" with the names for the table columns
		if len(queryColumnNames) == 1 && queryColumnNames[0] == "*" {
			for number := range tableColumns {
				queryColumnNumbers = append(queryColumnNumbers, number)
			}
		} else {
			// translate the column names from the query to the column numbers
			for _, queryColumnName := range queryColumnNames {
				found := false
				for number, column := range tableColumns {
					if strings.EqualFold(queryColumnName, column.Name) {
						queryColumnNumbers = append(queryColumnNumbers, number)
						found = true
						break
					}
				}
				if !found {
					log.Fatal("no such column: ", queryColumnName)
				}
			}
		}
	}

	// integer primary keys are stored as null and aliased with the rowid
	aliasedPKColumnNumber := -1
outer:
	for columnNumber, columnDef := range tableColumns {
		if strings.EqualFold(columnDef.Type, "INTEGER") && len(columnDef.Constraints) > 0 {
			for _, constraint := range columnDef.Constraints {
				if strings.Contains(strings.ToUpper(constraint), "PRIMARY KEY") {
					aliasedPKColumnNumber = columnNumber
					break outer
				}
			}
		}
	}

	filterColumnNumber := -1
	filterIndexPage := -1
	indexSortOrder := 1
	if filterColumnName == "rowid" {
		filterColumnNumber = aliasedPKColumnNumber
	} else if filterColumnName != "" {
		found := false
		for number, column := range tableColumns {
			if strings.EqualFold(filterColumnName, column.Name) {
				filterColumnNumber = number
				found = true
				break
			}
		}
		if !found {
			log.Fatal("no such column:", filterColumnName)
		}

		for _, entry := range db.Schema {
			if entry.Type == "index" && strings.EqualFold(queryTableName, entry.TableName) &&
				len(entry.Columns) > 0 && strings.EqualFold(filterColumnName, entry.Columns[0].Name) {
				filterIndexPage = entry.RootPage
				if strings.EqualFold(entry.Columns[0].Type, "DESC") {
					indexSortOrder = -1
				}
				break
			}
		}

		// "without rowid" table?
		if filterIndexPage == -1 {
			for _, constraint := range tableColumns[filterColumnNumber].Constraints {
				if strings.EqualFold(constraint, "PRIMARY") {
					filterIndexPage = rootPage
					break
				}
			}
		}
	}

	var tableData []TableRecord
	if filterIndexPage == -1 {
		tableData = db.fullTableScan(rootPage)
	} else {
		if filterColumnNumber == aliasedPKColumnNumber {
			row := db.getRecordByRowid(rootPage, filterValue.(int64))
			if row != nil {
				tableData = append(tableData, *row)
			}
		} else if filterIndexPage == rootPage {
			row := db.getRecordByPK(rootPage, filterColumnNumber, filterValue)
			if row != nil {
				tableData = append(tableData, *row)
			}
		} else {
			tableData = db.indexedTableScan(rootPage, filterIndexPage, filterValue, indexSortOrder)
		}
	}

	rowCount := 0
	for _, tableRow := range tableData {
		if aliasedPKColumnNumber >= 0 {
			tableRow.Columns[aliasedPKColumnNumber] = tableRow.Rowid
		}
		if filterColumnNumber >= 0 && compareAny(tableRow.Columns[filterColumnNumber], filterValue) != 0 {
			continue
		}
		if countingOnly {
			rowCount++
			continue
		}
		for i, columnNumber := range queryColumnNumbers {
			if i > 0 {
				fmt.Fprint(writer, "|")
			}
			var data any
			if columnNumber < len(tableRow.Columns) {
				data = tableRow.Columns[columnNumber]
			}
			// TODO: Implement default value (https://www.sqlite.org/lang_createtable.html#dfltval)

			// BLOB conversion for displaying
			if bytes, ok := data.([]byte); ok {
				data = string(bytes)
			}
			fmt.Fprint(writer, data)
		}
		fmt.Fprintln(writer)
	}

	if countingOnly {
		fmt.Fprintln(writer, rowCount)
	}
}
