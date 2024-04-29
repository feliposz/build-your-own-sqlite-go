package main

import (
	"fmt"
	"os"
	"strings"
)

var debugMode bool

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
