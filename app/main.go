package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"slices"
	"strings"
)

var debugMode bool

// Usage: your_sqlite3.sh sample.db .dbinfo
func main() {
	if len(os.Args) < 3 {
		fmt.Printf("usage: %s <database.db> <command>\n", os.Args[0])
		os.Exit(1)
	}

	databaseFilePath := os.Args[1]
	command := os.Args[2]

	db := NewDbContext(databaseFilePath)
	defer db.Close()

	switch command {
	case ".dbinfo":
		db.PrintDbInfo()
	case ".tables":
		db.PrintTables()
	case ".indexes":
		db.PrintIndexes()
	case ".schema":
		db.PrintSchema()
	default:
		if strings.Contains(strings.ToUpper(command), "SELECT") {
			db.HandleSelect(command)
		} else {
			fmt.Println("Unknown command", command)
			os.Exit(1)
		}
	}
}

type DbContext struct {
	File   *os.File
	Info   *DbInfo
	Schema []SchemaEntry
}

type DbInfo struct {
	DatabasePageSize           int
	WriteFormat                uint8
	ReadFormat                 uint8
	ReservedBytes              uint8
	MaxEmbeddedPayloadFraction uint8
	MinEmbeddedPayloadFraction uint8
	LeafPayloadFraction        uint8
	FileChangeCounter          uint32
	DatabasePageCount          uint32
	FirstFreeListPage          uint32
	FreelistPageCount          uint32
	SchemaCookie               uint32
	SchemaFormat               uint32
	DefaultCacheSize           uint32
	AutovacuumTopRoot          uint32
	IncrementalVacuum          uint32
	TextEncoding               uint32
	UserVersion                uint32
	ApplicationID              uint32
	SoftwareVersion            uint32
	NumberOfTables             uint32
	NumberOfIndexes            uint32
	NumberOfTriggers           uint32
	NumberOfViews              uint32
	SchemaSize                 uint32
	DataVersion                uint32
}

type SchemaEntry struct {
	Type        string
	Name        string
	TableName   string
	RootPage    int
	SQL         string
	Columns     []ColumnDef
	Constraints []string
}

type ColumnDef struct {
	Name        string
	Type        string
	Constraints []string
}

type PageHeader struct {
	PageType               uint8
	FirstFreeBlock         uint16
	CellCount              uint16
	StartOfCellContentArea uint32
	FragmentedFreeBytes    uint8
	CellPointerArrayOffset uint32
	RightMostPointer       uint32
	UnallocatedRegionSize  uint32
}

type TableRecord struct {
	Rowid   int64
	Columns []any
}

type TableRawRecord struct {
	Rowid int64
	Data  []byte
}

type InteriorTableEntry struct {
	childPage uint32
	key       int64
}

type InteriorIndexEntry struct {
	childPage  uint32
	keyPayload []byte
}

func readBigEndianUint16(b []byte) uint16 {
	return uint16(b[0])<<8 | uint16(b[1])
}

func readBigEndianUint32(b []byte) uint32 {
	return uint32(b[0])<<24 | uint32(b[1])<<16 | uint32(b[2])<<8 | uint32(b[3])
}

func readBigEndianVarint(data []byte) (value int64, size int) {
	for size < 9 {
		size++
		if size == 9 {
			value = (value << 8) | int64(data[size-1])
			break
		} else {
			value = (value << 7) | (int64(data[size-1]) & 0b01111111)
		}
		if (data[size-1]>>7)&1 == 0 {
			break
		}
	}
	return
}

func readBigEndianInt(data []byte) (value int64) {
	for _, b := range data {
		value = (value << 8) | int64(b)
	}
	return value
}

func NewDbContext(databaseFilePath string) *DbContext {
	db := &DbContext{}
	file, err := os.Open(databaseFilePath)
	if err != nil {
		log.Fatal(err)
	}
	db.File = file
	db.readDbInfo()
	db.readSchema()
	return db
}

func (db *DbContext) Close() {
	db.File.Close()
}

func (db *DbContext) readDbInfo() {

	header := make([]byte, 100)

	_, err := db.File.Read(header)
	if err != nil {
		log.Fatal(err)
	}

	if string(header[0:16]) != "SQLite format 3\000" {
		log.Fatal("Not a valid SQLite 3 file")
	}

	var info DbInfo

	pageSize := readBigEndianUint16(header[16:18])
	info.DatabasePageSize = int(pageSize)
	if pageSize == 0 {
		info.DatabasePageSize = 65536
	}

	info.WriteFormat = header[18]
	info.ReadFormat = header[19]
	info.ReservedBytes = header[20]
	info.MaxEmbeddedPayloadFraction = header[21]
	info.MinEmbeddedPayloadFraction = header[22]
	info.LeafPayloadFraction = header[23]
	info.FileChangeCounter = readBigEndianUint32(header[24:28])
	info.DatabasePageCount = readBigEndianUint32(header[28:32])
	info.FirstFreeListPage = readBigEndianUint32(header[32:36])
	info.FreelistPageCount = readBigEndianUint32(header[36:40])
	info.SchemaCookie = readBigEndianUint32(header[40:44])
	info.SchemaFormat = readBigEndianUint32(header[44:48])
	info.DefaultCacheSize = readBigEndianUint32(header[48:52])
	info.AutovacuumTopRoot = readBigEndianUint32(header[52:56])
	info.TextEncoding = readBigEndianUint32(header[56:60])
	info.UserVersion = readBigEndianUint32(header[60:64])
	info.IncrementalVacuum = readBigEndianUint32(header[64:68])
	info.ApplicationID = readBigEndianUint32(header[68:72])
	info.DataVersion = readBigEndianUint32(header[92:96])
	info.SoftwareVersion = readBigEndianUint32(header[96:100])

	db.Info = &info
}

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
	fmt.Printf("data version:        %d\n", info.DataVersion)
}

func (db *DbContext) readSchema() {
	schemaTableData := db.fullTableScan(1)

	schema := []SchemaEntry{}
	for _, row := range schemaTableData {
		entry := SchemaEntry{
			Type:      row.Columns[0].(string),
			Name:      row.Columns[1].(string),
			TableName: row.Columns[2].(string),
			RootPage:  int(row.Columns[3].(int64)),
			SQL:       row.Columns[4].(string),
		}
		switch entry.Type {
		case "table":
			db.Info.NumberOfTables++
			entry.Columns, entry.Constraints = parseColumns(entry.SQL)
		case "trigger":
			db.Info.NumberOfTriggers++
		case "view":
			db.Info.NumberOfViews++
			entry.Columns, entry.Constraints = parseColumns(entry.SQL)
		case "index":
			db.Info.NumberOfIndexes++
			entry.Columns, _ = parseColumns(entry.SQL)
		}
		schema = append(schema, entry)
	}
	db.Schema = schema
}

func parseColumns(sql string) ([]ColumnDef, []string) {
	columns := []ColumnDef{}
	constraints := []string{}
	if !strings.HasPrefix(sql, "CREATE") {
		log.Fatal("invalid DDL statement")
	}
	// remove everything before first "(" and last ")"
	sql = sql[strings.Index(sql, "(")+1:]
	sql = sql[:strings.LastIndex(sql, ")")]
	// TODO: type names can have "," inside them! parse this properly!!!
	// TODO: handle quoted names
	for _, columnDefinition := range strings.Split(sql, ",") {
		columnDefinition = strings.TrimSpace(columnDefinition)
		parts := strings.Split(columnDefinition, " ")
		switch strings.ToUpper(parts[0]) {
		case "PRIMARY", "CONSTRAINT", "UNIQUE", "CHECK", "FOREIGN":
			constraints = append(constraints, columnDefinition)
		default:
			column := ColumnDef{}
			column.Name = parts[0]
			if len(parts) > 1 {
				column.Type = parts[1]
			}
			if len(parts) > 2 {
				column.Constraints = parts[2:]
			}
			columns = append(columns, column)
		}
	}
	return columns, constraints
}

func (db *DbContext) getPage(pageNumber int) (header PageHeader, page []byte) {
	info := db.Info
	if pageNumber < 1 || pageNumber > int(info.DatabasePageCount) {
		log.Fatal("invalid page number:", pageNumber)
	}

	_, err := db.File.Seek(int64(pageNumber-1)*int64(info.DatabasePageSize), io.SeekStart)
	if err != nil {
		log.Fatal(err)
	}

	pageOffset := 0
	if pageNumber == 1 {
		// skip database header for root page
		pageOffset += 100
	}

	page = make([]byte, info.DatabasePageSize)
	_, err = db.File.Read(page)
	if err != nil {
		log.Fatal(err)
	}

	header.PageType = page[pageOffset]
	switch header.PageType {
	case 0x02, 0x05, 0x0a, 0x0d:
		// ok
	default:
		log.Fatal("invalid page type:", header.PageType)
	}

	// parsing b-tree page header

	header.FirstFreeBlock = readBigEndianUint16(page[pageOffset+1 : pageOffset+3])
	header.CellCount = readBigEndianUint16(page[pageOffset+3 : pageOffset+5])
	header.StartOfCellContentArea = uint32(readBigEndianUint16(page[pageOffset+5 : pageOffset+7]))
	if header.StartOfCellContentArea == 0 {
		header.StartOfCellContentArea = 65536
	}
	header.FragmentedFreeBytes = page[pageOffset+7]
	header.CellPointerArrayOffset = 8
	if header.PageType == 0x02 || header.PageType == 0x05 {
		header.RightMostPointer = readBigEndianUint32(page[pageOffset+8 : pageOffset+12])
		header.CellPointerArrayOffset += 4
	}
	header.UnallocatedRegionSize = header.StartOfCellContentArea - (header.CellPointerArrayOffset + uint32(header.CellCount)*2)

	// account for the db header if needed
	header.CellPointerArrayOffset += uint32(pageOffset)

	if debugMode {
		fmt.Printf("---------- page header ----------\n")
		fmt.Printf("pageType:               %v\n", header.PageType)
		fmt.Printf("firstFreeBlock:         %v\n", header.FirstFreeBlock)
		fmt.Printf("cellCount:              %v\n", header.CellCount)
		fmt.Printf("startOfCellContentArea: %v\n", header.StartOfCellContentArea)
		fmt.Printf("fragmentedFreeBytes:    %v\n", header.FragmentedFreeBytes)
		fmt.Printf("rightMostPointer:       %v\n", header.RightMostPointer)
		fmt.Printf("unallocatedRegionSize:  %v\n", header.UnallocatedRegionSize)
	}
	return
}

func getCellOffsets(pageHeader PageHeader, page []byte) (offsets []int) {
	for cell := uint16(0); cell < pageHeader.CellCount; cell++ {
		cellPointerOffset := pageHeader.CellPointerArrayOffset + uint32(cell*2)
		cellOffset := int(readBigEndianUint16(page[cellPointerOffset : cellPointerOffset+2]))
		offsets = append(offsets, cellOffset)
	}
	return
}

func getInteriorTableEntries(pageHeader PageHeader, page []byte) (entries []InteriorTableEntry) {
	if debugMode {
		fmt.Printf("cell\tpointer\tpage\tkey\n")
	}
	for cell, cellPointer := range getCellOffsets(pageHeader, page) {
		offset := cellPointer
		leftChildPage := readBigEndianUint32(page[offset : offset+4])
		offset += 4
		key, bytes := readBigEndianVarint(page[offset:])
		offset += bytes
		if debugMode {
			fmt.Printf("%v\t%04x\t%v\t%v\n", cell, cellPointer, leftChildPage, key)
		}
		entries = append(entries, InteriorTableEntry{leftChildPage, key})
	}
	entries = append(entries, InteriorTableEntry{pageHeader.RightMostPointer, -1})

	return
}

func getLeafTableRecords(pageHeader PageHeader, page []byte) (tableData []TableRecord) {

	// reading each cell pointer array
	if debugMode {
		fmt.Printf("cell\tpointer\tpayload\trowid\tcontent\n")
	}
	for cell, cellPointer := range getCellOffsets(pageHeader, page) {
		offset := cellPointer
		payloadSize, bytes := readBigEndianVarint(page[offset : offset+9])
		offset += bytes
		rowid, bytes := readBigEndianVarint(page[offset : offset+9])
		offset += bytes
		// TODO: not handling overflow!
		record := page[offset : offset+int(payloadSize)]
		if debugMode {
			fmt.Printf("%v\t%04x\t%v\t%v\t", cell, cellPointer, payloadSize, rowid)
		}

		columnData := parseRecordFormat(record)
		tableData = append(tableData, TableRecord{Rowid: rowid, Columns: columnData})
	}

	return
}

func parseRecordFormat(record []byte) []any {
	// determine column type and lenghts from record header
	recordHeaderSize, bytes := readBigEndianVarint(record)
	index := bytes
	columnTypeLengths := [][2]int{}
	for index < int(recordHeaderSize) {
		typeCode, bytes := readBigEndianVarint(record[index:recordHeaderSize])
		var typeLength [2]int
		switch typeCode {
		case 0:
			typeLength = [2]int{0, 0}
		case 1:
			typeLength = [2]int{1, 1}
		case 2:
			typeLength = [2]int{1, 2}
		case 3:
			typeLength = [2]int{1, 3}
		case 4:
			typeLength = [2]int{1, 4}
		case 5:
			typeLength = [2]int{1, 6}
		case 6:
			typeLength = [2]int{1, 8}
		case 7:
			typeLength = [2]int{2, 8}
		case 8:
			typeLength = [2]int{8, 0}
		case 9:
			typeLength = [2]int{9, 0}
		case 10, 11:
			typeLength = [2]int{int(typeCode), 1}
		default:
			if typeCode < 12 {
				log.Fatal("invalid column type code: ", typeCode)
			}
			if typeCode%2 == 0 {
				typeLength = [2]int{12, (int(typeCode) - 12) / 2}
			} else {
				typeLength = [2]int{13, (int(typeCode) - 13) / 2}
			}
		}
		columnTypeLengths = append(columnTypeLengths, typeLength)
		index += bytes
	}

	// reading data according to format/length

	columnData := []any{}
	for _, typeLength := range columnTypeLengths {
		switch typeLength[0] {
		case 0:
			columnData = append(columnData, nil)
		case 1:
			integer := readBigEndianInt(record[index : index+typeLength[1]])
			columnData = append(columnData, integer)
		case 2:
			log.Fatal("float not implemented!")
		case 8:
			columnData = append(columnData, int64(0))
		case 9:
			columnData = append(columnData, int64(1))
		case 12:
			columnData = append(columnData, record[index:index+typeLength[1]])
		case 13:
			columnData = append(columnData, string(record[index:index+typeLength[1]]))
		}
		index += typeLength[1]
	}
	if debugMode {
		fmt.Printf("%#v\n", columnData)
	}
	return columnData
}

func (db *DbContext) PrintTables() {
	for _, entry := range db.Schema {
		if entry.Type == "table" && !strings.HasPrefix(entry.Name, "sqlite_") {
			fmt.Print(entry.Name, " ")
		}
	}
	fmt.Println()
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
		fmt.Println(entry.SQL)
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

	var filterColumnName, filterValue string
	if wherePos == -1 {
		wherePos = len(query)
	} else {
		whereParts := strings.Split(query[wherePos+5:], "=")
		filterColumnName = strings.TrimSpace(whereParts[0])
		filterValue = strings.Trim(strings.TrimSpace(whereParts[1]), "'")
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
			break
		}
	}

	if rootPage == 0 {
		log.Fatal("no such table:", queryTableName)
	}

	// TODO: this will not work when WHERE clause is used, must process records
	if strings.EqualFold(queryColumnNames[0], "COUNT(*)") {
		rowCount := db.countRows(rootPage)
		fmt.Println(rowCount)
		return
	}

	// replace "*" with the names for the table columns
	if len(queryColumnNames) == 1 && queryColumnNames[0] == "*" {
		queryColumnNames = nil
		for _, column := range tableColumns {
			queryColumnNames = append(queryColumnNames, column.Name)
		}
	}

	// translate the column names from the query to the column numbers

	queryColumnNumbers := []int{}
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
			log.Fatal("no such column:", queryColumnName)
		}
	}

	filterColumnNumber := -1
	filterIndexPage := -1
	if filterColumnName != "" {
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
			if entry.Type == "index" && strings.EqualFold(queryTableName, entry.TableName) && strings.EqualFold(filterColumnName, entry.Columns[0].Name) {
				filterIndexPage = entry.RootPage
				break
			}
		}
	}

	var tableData []TableRecord
	if filterIndexPage == -1 {
		tableData = db.fullTableScan(rootPage)
	} else {
		// TODO: implement multiple-key indexes search
		// TODO: implement non-string key types
		tableData = db.indexedTableScan(rootPage, filterIndexPage, filterValue)
	}

	for _, tableRow := range tableData {
		if filterColumnNumber >= 0 && tableRow.Columns[filterColumnNumber] != filterValue {
			continue
		}
		for i, columnNumber := range queryColumnNumbers {
			if i > 0 {
				fmt.Print("|")
			}
			data := tableRow.Columns[columnNumber]
			if data == nil {
				columnDef := tableColumns[columnNumber]
				// autoincrement integer primary keys are stored as null and aliased with the rowid
				// TODO: properly check for primary key and do this only once, not for every row!!!
				if strings.EqualFold(columnDef.Type, "integer") && len(columnDef.Constraints) > 0 && strings.EqualFold(columnDef.Constraints[0], "primary") {
					data = tableRow.Rowid
				}
			}
			fmt.Print(data)
		}
		fmt.Println()
	}

}

func (db *DbContext) countRows(rootPage int) int {
	// TODO: traverse b-tree
	pageHeader, _ := db.getPage(rootPage)
	// only considering b-tree table leaf pages for now
	if pageHeader.PageType != 0x0d {
		log.Fatal("page type not implemented")
	}
	return int(pageHeader.CellCount)
}

func (db *DbContext) fullTableScan(rootPage int) []TableRecord {
	var tableData []TableRecord
	db.walkBtreeTablePages(rootPage, &tableData)
	return tableData
}

func (db *DbContext) walkBtreeTablePages(page int, tableDataPtr *[]TableRecord) {
	header, data := db.getPage(page)
	if header.PageType == 0x05 {
		entries := getInteriorTableEntries(header, data)
		for _, entry := range entries {
			db.walkBtreeTablePages(int(entry.childPage), tableDataPtr)
		}
	} else if header.PageType == 0x0d {
		records := getLeafTableRecords(header, data)
		*tableDataPtr = append(*tableDataPtr, records...)
	} else {
		log.Fatal("unexpected page type when walking btree: ", header.PageType)
	}
}

func (db *DbContext) indexedTableScan(rootPage, filterIndexPage int, filterValue string) []TableRecord {
	var rowids []int64
	var tableData []TableRecord
	db.walkBtreeIndexPages(filterIndexPage, filterValue, &rowids)
	slices.Sort(rowids)
	for _, rowid := range rowids {
		// starting from table root page, binary search for each rowid and retrieve only the filtered records
		// implement the most dumb form (may retrieve pages multiple times)
		record := db.getRecordByRowid(rootPage, rowid)
		if record == nil {
			log.Fatal("unexpected missing rowid: ", rowid)
		}
		tableData = append(tableData, *record)
	}
	// TODO: try to come up with a "clever" way to retrieve multiple rowids if present on the same searched page
	return tableData
}

func (db *DbContext) walkBtreeIndexPages(page int, filterValue string, rowids *[]int64) {
	header, data := db.getPage(page)
	if header.PageType == 0x02 {
		entries := getInteriorIndexEntries(header, data)
		lo, hi := 0, len(entries)-1
		for lo <= hi {
			mid := (lo + hi) / 2
			if mid == len(entries)-1 {
				// right-most child
				lo = mid
				break
			}
			key := parseRecordFormat(entries[mid].keyPayload)
			if key[0].(string) == filterValue {
				// NOTE: the interior page itself also point to a valid row that is NOT on the leaf page!
				*rowids = append(*rowids, key[1].(int64))
				lo = mid
				break
			} else if filterValue < key[0].(string) {
				hi = mid - 1
			} else {
				lo = mid + 1
			}
		}
		// TODO: how to properly check for keys that have records on more than one page?
		for i := lo; i <= lo+1 && i < len(entries); i++ {
			db.walkBtreeIndexPages(int(entries[i].childPage), filterValue, rowids)
		}
	} else if header.PageType == 0x0a {
		entries := getLeafIndexEntries(header, data)
		lo, hi := 0, len(entries)-1
		for lo <= hi {
			mid := (lo + hi) / 2
			key := parseRecordFormat(entries[mid])
			if filterValue <= key[0].(string) {
				hi = mid - 1
			} else {
				lo = mid + 1
			}
		}
		for i := lo; i < len(entries); i++ {
			key := parseRecordFormat(entries[i])
			if key[0].(string) > filterValue {
				break
			}
			*rowids = append(*rowids, key[1].(int64))
		}
	} else {
		log.Fatal("unexpected page type when walking btree: ", header.PageType)
	}
}

func getInteriorIndexEntries(pageHeader PageHeader, page []byte) (entries []InteriorIndexEntry) {
	if debugMode {
		fmt.Printf("cell\tpointer\tpage\tpayload\n")
	}

	for cell, cellPointer := range getCellOffsets(pageHeader, page) {
		offset := cellPointer
		leftChildPage := readBigEndianUint32(page[offset : offset+4])
		offset += 4
		// TODO: handle overflow!
		payloadSize, bytes := readBigEndianVarint(page[offset:])
		offset += bytes
		keyPayload := page[offset : offset+int(payloadSize)]
		if debugMode {
			fmt.Printf("%v\t%04x\t%v\t%q\n", cell, cellPointer, leftChildPage, keyPayload)
		}
		entries = append(entries, InteriorIndexEntry{leftChildPage, keyPayload})
	}
	entries = append(entries, InteriorIndexEntry{pageHeader.RightMostPointer, nil})

	return
}

func getLeafIndexEntries(pageHeader PageHeader, page []byte) (records [][]byte) {
	// reading each cell pointer array
	if debugMode {
		fmt.Printf("cell\tpointer\tkey\n")
	}
	for cell, cellPointer := range getCellOffsets(pageHeader, page) {
		offset := cellPointer
		// TODO: handle overflow!
		payloadSize, bytes := readBigEndianVarint(page[offset:])
		offset += bytes
		keyPayload := page[offset : offset+int(payloadSize)]
		if debugMode {
			fmt.Printf("%v\t%04x\t%v\n", cell, cellPointer, keyPayload)
		}
		records = append(records, keyPayload)
	}

	return
}

func (db *DbContext) getRecordByRowid(page int, rowid int64) *TableRecord {
	header, data := db.getPage(page)
	if header.PageType == 0x05 {

		entries := getInteriorTableEntries(header, data)
		lo, hi := 0, len(entries)-1
		for lo <= hi {
			mid := (lo + hi) / 2
			if mid == len(entries)-1 {
				// right-most child
				lo = mid
				break
			} else if entries[mid].key == rowid {
				lo = mid
				break
			} else if rowid < entries[mid].key {
				hi = mid - 1
			} else {
				lo = mid + 1
			}
		}
		return db.getRecordByRowid(int(entries[lo].childPage), rowid)

	} else if header.PageType == 0x0d {
		rawRecords := getLeafTableRawRecords(header, data)
		lo, hi := 0, len(rawRecords)-1
		for lo <= hi {
			mid := (lo + hi) / 2
			if rawRecords[mid].Rowid == rowid {
				record := parseRecordFormat(rawRecords[mid].Data)
				return &TableRecord{rowid, record}
			} else if rowid < rawRecords[mid].Rowid {
				hi = mid - 1
			} else {
				lo = mid + 1
			}
		}
	} else {
		log.Fatal("unexpected page type when walking btree: ", header.PageType)
	}
	return nil
}

func getLeafTableRawRecords(pageHeader PageHeader, page []byte) (records []TableRawRecord) {
	for _, offset := range getCellOffsets(pageHeader, page) {
		payloadSize, bytes := readBigEndianVarint(page[offset : offset+9])
		offset += bytes
		rowid, bytes := readBigEndianVarint(page[offset : offset+9])
		offset += bytes
		// TODO: not handling overflow!
		record := page[offset : offset+int(payloadSize)]
		records = append(records, TableRawRecord{rowid, record})
	}
	return
}
