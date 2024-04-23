package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"os"
	// Available if you need it!
	// "github.com/xwb1989/sqlparser"
)

// Usage: your_sqlite3.sh sample.db .dbinfo
func main() {
	databaseFilePath := os.Args[1]
	command := os.Args[2]

	switch command {
	case ".dbinfo":
		info := readDBInfo(databaseFilePath)
		printDbInfo(info)
	default:
		fmt.Println("Unknown command", command)
		os.Exit(1)
	}
}

type DBInfo struct {
	DatabasePageSize           uint16
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

func readDBInfo(databaseFilePath string) *DBInfo {

	databaseFile, err := os.Open(databaseFilePath)
	if err != nil {
		log.Fatal(err)
	}

	header := make([]byte, 100)

	_, err = databaseFile.Read(header)
	if err != nil {
		log.Fatal(err)
	}

	if string(header[0:16]) != "SQLite format 3\000" {
		log.Fatal("Not a valid SQLite 3 file")
	}

	var info DBInfo
	if err := binary.Read(bytes.NewReader(header[16:18]), binary.BigEndian, &info.DatabasePageSize); err != nil {
		log.Fatal("Failed to read DatabasePageSize:", err)
	}
	if err := binary.Read(bytes.NewReader(header[18:19]), binary.BigEndian, &info.WriteFormat); err != nil {
		log.Fatal("Failed to read WriteFormat:", err)
	}
	if err := binary.Read(bytes.NewReader(header[19:20]), binary.BigEndian, &info.ReadFormat); err != nil {
		log.Fatal("Failed to read ReadFormat:", err)
	}
	if err := binary.Read(bytes.NewReader(header[20:21]), binary.BigEndian, &info.ReservedBytes); err != nil {
		log.Fatal("Failed to read ReservedBytes:", err)
	}
	if err := binary.Read(bytes.NewReader(header[21:22]), binary.BigEndian, &info.MaxEmbeddedPayloadFraction); err != nil {
		log.Fatal("Failed to read MaxEmbeddedPayloadFraction:", err)
	}
	if err := binary.Read(bytes.NewReader(header[22:23]), binary.BigEndian, &info.MinEmbeddedPayloadFraction); err != nil {
		log.Fatal("Failed to read MinEmbeddedPayloadFraction:", err)
	}
	if err := binary.Read(bytes.NewReader(header[23:24]), binary.BigEndian, &info.LeafPayloadFraction); err != nil {
		log.Fatal("Failed to read LeafPayloadFraction:", err)
	}
	if err := binary.Read(bytes.NewReader(header[24:28]), binary.BigEndian, &info.FileChangeCounter); err != nil {
		log.Fatal("Failed to read FileChangeCounter:", err)
	}
	if err := binary.Read(bytes.NewReader(header[28:32]), binary.BigEndian, &info.DatabasePageCount); err != nil {
		log.Fatal("Failed to read DatabasePageCount:", err)
	}
	if err := binary.Read(bytes.NewReader(header[32:36]), binary.BigEndian, &info.FirstFreeListPage); err != nil {
		log.Fatal("Failed to read FirstFreeListPage:", err)
	}
	if err := binary.Read(bytes.NewReader(header[36:40]), binary.BigEndian, &info.FreelistPageCount); err != nil {
		log.Fatal("Failed to read FreelistPageCount:", err)
	}
	if err := binary.Read(bytes.NewReader(header[40:44]), binary.BigEndian, &info.SchemaCookie); err != nil {
		log.Fatal("Failed to read SchemaCookie:", err)
	}
	if err := binary.Read(bytes.NewReader(header[44:48]), binary.BigEndian, &info.SchemaFormat); err != nil {
		log.Fatal("Failed to read SchemaFormat:", err)
	}
	if err := binary.Read(bytes.NewReader(header[48:52]), binary.BigEndian, &info.DefaultCacheSize); err != nil {
		log.Fatal("Failed to read DefaultCacheSize:", err)
	}
	if err := binary.Read(bytes.NewReader(header[52:56]), binary.BigEndian, &info.AutovacuumTopRoot); err != nil {
		log.Fatal("Failed to read AutovacuumTopRoot:", err)
	}
	if err := binary.Read(bytes.NewReader(header[56:60]), binary.BigEndian, &info.TextEncoding); err != nil {
		log.Fatal("Failed to read TextEncoding:", err)
	}
	if err := binary.Read(bytes.NewReader(header[60:64]), binary.BigEndian, &info.UserVersion); err != nil {
		log.Fatal("Failed to read UserVersion:", err)
	}
	if err := binary.Read(bytes.NewReader(header[64:68]), binary.BigEndian, &info.IncrementalVacuum); err != nil {
		log.Fatal("Failed to read IncrementalVacuum:", err)
	}
	if err := binary.Read(bytes.NewReader(header[68:72]), binary.BigEndian, &info.ApplicationID); err != nil {
		log.Fatal("Failed to read ApplicationID:", err)
	}
	if err := binary.Read(bytes.NewReader(header[92:96]), binary.BigEndian, &info.DataVersion); err != nil {
		log.Fatal("Failed to read DataVersion:", err)
	}
	if err := binary.Read(bytes.NewReader(header[96:100]), binary.BigEndian, &info.SoftwareVersion); err != nil {
		log.Fatal("Failed to read SoftwareVersion:", err)
	}
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
