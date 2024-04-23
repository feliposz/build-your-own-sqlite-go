package main

import (
	"fmt"
	"io"
	"log"
	"os"
	// Available if you need it!
	// "github.com/xwb1989/sqlparser"
)

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
		printDbInfo(info)
		readSchemaInfo(databaseFile, info)
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

	firstFreeBlock := readBigEndianUint16(page[1:3])
	cellCount := readBigEndianUint16(page[3:5])
	startOfCellContentArea := int(readBigEndianUint16(page[5:7]))
	if startOfCellContentArea == 0 {
		startOfCellContentArea = 65536
	}
	fragmentedFreeBytes := page[7]
	var rightMostPointer uint32
	if pageType == 0x02 || pageType == 0x05 {
		rightMostPointer = readBigEndianUint32(page[5:7])
	}

	fmt.Println(pageType, firstFreeBlock, cellCount, startOfCellContentArea, fragmentedFreeBytes, rightMostPointer)

}
