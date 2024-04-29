package main

import (
	"fmt"
	"log"
	"slices"
	"strconv"
	"strings"
)

func (db *DbContext) PrintDbInfo() {
	info := db.Info
	encodingDescription := "?"
	switch info.TextEncoding {
	case 1:
		encodingDescription = "utf8"
	case 2:
		encodingDescription = "utf16le"
	case 3:
		encodingDescription = "utf16be"
	}
	fmt.Printf("database page size:  %d\n", info.DatabasePageSize)
	fmt.Printf("write format:        %d\n", info.WriteFormat)
	fmt.Printf("read format:         %d\n", info.ReadFormat)
	fmt.Printf("reserved bytes:      %d\n", info.ReservedBytes)
	fmt.Printf("file change counter: %d\n", info.FileChangeCounter)
	fmt.Printf("database page count: %d\n", info.DatabasePageCount)
	fmt.Printf("freelist page count: %d\n", info.FreelistPageCount)
	fmt.Printf("schema cookie:       %d\n", info.SchemaCookie)
	fmt.Printf("schema format:       %d\n", info.SchemaFormat)
	fmt.Printf("default cache size:  %d\n", info.DefaultCacheSize)
	fmt.Printf("autovacuum top root: %d\n", info.AutovacuumTopRoot)
	fmt.Printf("incremental vacuum:  %d\n", info.IncrementalVacuum)
	fmt.Printf("text encoding:       %d (%v)\n", info.TextEncoding, encodingDescription)
	fmt.Printf("user version:        %d\n", info.UserVersion)
	fmt.Printf("application id:      %d\n", info.ApplicationID)
	fmt.Printf("software version:    %d\n", info.SoftwareVersion)
	fmt.Printf("number of tables:    %d\n", info.NumberOfTables)
	fmt.Printf("number of indexes:   %d\n", info.NumberOfIndexes)
	fmt.Printf("number of triggers:  %d\n", info.NumberOfTriggers)
	fmt.Printf("number of views:     %d\n", info.NumberOfViews)
	fmt.Printf("schema size:         %d\n", info.SchemaSize)
}

func (db *DbContext) PrintTables() {
	tables := []string{}
	for _, entry := range db.Schema {
		if (entry.Type == "table" || entry.Type == "view") && !strings.HasPrefix(entry.Name, "sqlite_") {
			tables = append(tables, entry.Name)
		}
	}
	slices.Sort(tables)
	fmt.Println(strings.Join(tables, " "))
}

func (db *DbContext) PrintIndexes() {
	for _, entry := range db.Schema {
		if entry.Type == "index" {
			fmt.Print(entry.Name, " ")
		}
	}
	fmt.Println()
}

func (db *DbContext) PrintSchema() {
	for _, entry := range db.Schema {
		if entry.SQL != "" {
			fmt.Printf("%s;\n", entry.SQL)
		}
	}
}

func (db *DbContext) HandleSelect(query string) {

	// TODO: properly parse SQL syntax

	queryUpper := strings.ToUpper(query)
	selectPos := strings.Index(queryUpper, "SELECT")
	fromPos := strings.Index(queryUpper, "FROM")
	wherePos := strings.Index(queryUpper, "WHERE")

	if selectPos < 0 || fromPos < 0 || fromPos < selectPos {
		log.Fatal("syntax error")
	}

	var filterColumnName string
	var filterValue any
	if wherePos == -1 {
		wherePos = len(query)
	} else {
		whereParts := strings.SplitN(query[wherePos+5:], "=", 2)
		filterColumnName = strings.TrimSpace(whereParts[0])
		value := strings.TrimSpace(whereParts[1])
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
					panic(err)
				}
			}
		}
	}

	queryTableName := strings.TrimSpace(query[fromPos+4 : wherePos])
	queryColumnNames := strings.Split(query[selectPos+6:fromPos], ",")
	for i := range queryColumnNames {
		queryColumnNames[i] = strings.TrimSpace(queryColumnNames[i])
	}

	rootPage := 0
	var tableColumns []ColumnDef

	if strings.EqualFold(queryTableName, "sqlite_schema") || strings.EqualFold(queryTableName, "sqlite_master") {
		rootPage = 1
		// sqlite_schema has no table definition - this is the one from the docs: https://www.sqlite.org/fileformat.html#storage_of_the_sql_database_schema
		tableColumns, _ = parseColumns("CREATE TABLE sqlite_schema(type text, name text, tbl_name text, rootpage integer, sql text);")
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
			fmt.Println(rowCount)
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
		if strings.EqualFold(columnDef.Type, "integer") && len(columnDef.Constraints) > 0 {
			for _, constraint := range columnDef.Constraints {
				if strings.EqualFold(constraint, "primary") {
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
				fmt.Print("|")
			}
			var data any
			if columnNumber < len(tableRow.Columns) {
				data = tableRow.Columns[columnNumber]
			} else {
				// TODO: Implement default value (https://www.sqlite.org/lang_createtable.html#dfltval)
			}
			// BLOB conversion for displaying
			if bytes, ok := data.([]byte); ok {
				data = string(bytes)
			}
			fmt.Print(data)
		}
		fmt.Println()
	}

	if countingOnly {
		fmt.Println(rowCount)
	}
}
