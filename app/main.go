package main

import (
	"fmt"
	"io"
	"log"
	"os"
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

	switch command {
	case ".dbinfo":
		info := readDBInfo(databaseFile)
		readSchemaInfo(databaseFile, info)
		printDbInfo(info)
	default:
		fmt.Println("Unknown command", command)
		os.Exit(1)
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
	headerSize := 100
	_, err := databaseFile.Seek(int64(headerSize), io.SeekStart)
	if err != nil {
		log.Fatal(err)
	}

	page := make([]byte, info.DatabasePageSize-headerSize)
	databaseFile.Read(page)

	pageType := page[0]
	switch pageType {
	case 0x02:
	case 0x05:
	case 0x0a:
	case 0x0d:
	default:
		log.Fatal("invalid page type:", pageType)
	}

	// parsing b-tree page header

	firstFreeBlock := readBigEndianUint16(page[1:3])
	cellCount := readBigEndianUint16(page[3:5])
	startOfCellContentArea := int(readBigEndianUint16(page[5:7]))
	if startOfCellContentArea == 0 {
		startOfCellContentArea = 65536
	}
	fragmentedFreeBytes := page[7]
	cellPointerArrayOffset := 8
	var rightMostPointer uint32
	if pageType == 0x02 || pageType == 0x05 {
		rightMostPointer = readBigEndianUint32(page[8:12])
		cellPointerArrayOffset += 4
	}
	unallocatedRegionSize := startOfCellContentArea - (cellPointerArrayOffset + int(cellCount)*2)

	if debugMode {
		fmt.Printf("---------- page header ----------\n")
		fmt.Printf("pageType:               %v\n", pageType)
		fmt.Printf("firstFreeBlock:         %v\n", firstFreeBlock)
		fmt.Printf("cellCount:              %v\n", cellCount)
		fmt.Printf("startOfCellContentArea: %v\n", startOfCellContentArea)
		fmt.Printf("fragmentedFreeBytes:    %v\n", fragmentedFreeBytes)
		fmt.Printf("rightMostPointer:       %v\n", rightMostPointer)
		fmt.Printf("unallocatedRegionSize:  %v\n", unallocatedRegionSize)
	}

	// only considering b-tree table leaf pages for now
	if pageType != 0x0d {
		log.Fatal("page type not implemented")
	}

	// reading each cell pointer array

	rawRecords := [][]byte{}

	if debugMode {
		fmt.Printf("cell\tpointer\tpayload\trowid\tcontent\n")
	}
	for cell := 0; cell < int(cellCount); cell++ {
		cellPointerOffset := cellPointerArrayOffset + cell*2
		cellPointer := readBigEndianUint16(page[cellPointerOffset : cellPointerOffset+2])
		offset := int(cellPointer) - 100 // header?
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

	tableData := [][]any{}

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
				columnData = append(columnData, 0)
			case 9:
				columnData = append(columnData, 0)
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

	for _, row := range tableData {
		if row[0] == "table" {
			info.NumberOfTables++
		}
	}
}
