package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"slices"
	"strings"
	// Available if you need it!
	// "github.com/xwb1989/sqlparser"
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

	databaseFile, err := os.Open(databaseFilePath)
	if err != nil {
		log.Fatal(err)
	}
	defer databaseFile.Close()

	info := readDBInfo(databaseFile)
	readSchemaInfo(databaseFile, info)

	switch command {
	case ".dbinfo":
		printDbInfo(info)
	case ".tables":
		printTables(info)
	default:
		if strings.Contains(strings.ToUpper(command), "SELECT") {
			handleSelect(databaseFile, info, command)
		} else {
			fmt.Println("Unknown command", command)
			os.Exit(1)
		}
	}
}

type DBInfo struct {
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
	Schema                     []SchemaEntry
}

type SchemaEntry struct {
	Type      string
	Name      string
	TableName string
	RootPage  int
	SQL       string
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

func readBigEndianUint16(b []byte) uint16 {
	return uint16(b[0])<<8 | uint16(b[1])
}

func readBigEndianUint32(b []byte) uint32 {
	return uint32(b[0])<<24 | uint32(b[1])<<16 | uint32(b[2])<<8 | uint32(b[3])
}

func readBigEndianVarint(b []byte) (value int64, size int) {
	for size < 9 {
		size++
		if size == 9 {
			value = (value << 8) | int64(b[size-1])
			break
		} else {
			value = (value << 7) | (int64(b[size-1]) & 0b01111111)
		}
		if (b[size-1]>>7)&1 == 0 {
			break
		}
	}
	return
}

func readDBInfo(databaseFile *os.File) *DBInfo {

	header := make([]byte, 100)

	_, err := databaseFile.Read(header)
	if err != nil {
		log.Fatal(err)
	}

	if string(header[0:16]) != "SQLite format 3\000" {
		log.Fatal("Not a valid SQLite 3 file")
	}

	var info DBInfo

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
	return &info
}

func printDbInfo(info *DBInfo) {
	encodingDescription := "?"
	switch info.TextEncoding {
	case 1:
		encodingDescription = "utf-8"
	case 2:
		encodingDescription = "utf-16le"
	case 3:
		encodingDescription = "utf-16be"
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

func readSchemaInfo(databaseFile *os.File, info *DBInfo) {
	pageHeader, pageData := getPage(databaseFile, info, 1)

	// only considering b-tree table leaf pages for now
	if pageHeader.PageType != 0x0d {
		log.Fatal("page type not implemented")
	}

	rawRecords := getLeafTableRecords(pageHeader, pageData)
	schemaTableData := getTableData(rawRecords)
	updateSchemaInfo(info, schemaTableData)
}

func getPage(databaseFile *os.File, info *DBInfo, pageNumber int) (header PageHeader, page []byte) {
	if pageNumber < 1 || pageNumber > int(info.DatabasePageCount) {
		log.Fatal("invalid page number:", pageNumber)
	}

	_, err := databaseFile.Seek(int64(pageNumber-1)*int64(info.DatabasePageSize), io.SeekStart)
	if err != nil {
		log.Fatal(err)
	}

	pageOffset := 0
	if pageNumber == 1 {
		// skip database header for root page
		pageOffset += 100
	}

	page = make([]byte, info.DatabasePageSize)
	_, err = databaseFile.Read(page)
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

func getLeafTableRecords(pageHeader PageHeader, page []byte) (rawRecords [][]byte) {

	// reading each cell pointer array
	if debugMode {
		fmt.Printf("cell\tpointer\tpayload\trowid\tcontent\n")
	}
	for cell := uint16(0); cell < pageHeader.CellCount; cell++ {
		cellPointerOffset := pageHeader.CellPointerArrayOffset + uint32(cell*2)
		cellPointer := readBigEndianUint16(page[cellPointerOffset : cellPointerOffset+2])
		offset := int(cellPointer)
		payloadSize, bytes := readBigEndianVarint(page[offset : offset+9])
		offset += bytes
		rowid, bytes := readBigEndianVarint(page[offset : offset+9])
		offset += bytes
		// TODO: not handling overflow!
		content := page[offset : offset+int(payloadSize)]
		if debugMode {
			fmt.Printf("%v\t%04x\t%v\t%v\t%q\n", cell, cellPointer, payloadSize, rowid, content)
		}
		rawRecords = append(rawRecords, content)
	}

	return
}

func getTableData(rawRecords [][]byte) (tableData [][]any) {

	// parsing record format

	if debugMode {
		fmt.Printf("record\tdata\n")
	}
	for i, record := range rawRecords {

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
				integer, _ := readBigEndianVarint(record[index : index+typeLength[1]])
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
			fmt.Printf("%v\t%#v\n", i, columnData)
		}

		tableData = append(tableData, columnData)
	}

	return
}

func updateSchemaInfo(info *DBInfo, schemaTableData [][]any) {
	for _, row := range schemaTableData {
		// TODO: 'table', 'index', 'view', or 'trigger'
		if row[0] == "table" {
			info.NumberOfTables++
		}
		entry := SchemaEntry{row[0].(string), row[1].(string), row[2].(string), int(row[3].(int64)), row[4].(string)}
		info.Schema = append(info.Schema, entry)
	}
}

func printTables(info *DBInfo) {
	for _, entry := range info.Schema {
		if entry.Type == "table" {
			fmt.Print(entry.Name, " ")
		}
	}
	fmt.Println()
}

func handleSelect(databaseFile *os.File, info *DBInfo, command string) {

	// TODO: properly parse SQL syntax

	parts := strings.Split(command, " ")
	parts = slices.DeleteFunc(parts, func(s string) bool {
		return len(s) == 0
	})

	if len(parts) != 4 || strings.ToUpper(parts[0]) != "SELECT" || strings.ToUpper(parts[2]) != "FROM" {
		log.Fatal("syntax error")
	}

	if strings.ToUpper(parts[1]) != "COUNT(*)" {
		log.Fatal("not implemented")
	}

	tableName := parts[3]

	if tableName == "sqlite_schema" || tableName == "sqlite_master" {
		rowCount := countRows(databaseFile, info, 1)
		fmt.Println(rowCount)
		return
	}

	for _, entry := range info.Schema {
		if entry.Type == "table" && entry.Name == tableName {
			rowCount := countRows(databaseFile, info, entry.RootPage)
			fmt.Println(rowCount)
			return
		}
	}

	log.Fatal("no such table:", tableName)
}

func countRows(databaseFile *os.File, info *DBInfo, rootPage int) int {
	// TODO: traverse b-tree
	pageHeader, _ := getPage(databaseFile, info, rootPage)
	// only considering b-tree table leaf pages for now
	if pageHeader.PageType != 0x0d {
		log.Fatal("page type not implemented")
	}
	return int(pageHeader.CellCount)
}
