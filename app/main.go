package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

var debugMode bool

func main() {
	if len(os.Args) < 2 {
		fmt.Printf("usage: %s <database.db> [<command> ...]\n", os.Args[0])
		os.Exit(1)
	}

	databaseFilePath := os.Args[1]

	db := NewDbContext(databaseFilePath)
	defer db.Close()

	if len(os.Args) == 2 {
		repl(db)
	} else {
		for _, command := range os.Args[2:] {
			execute(db, command)
		}
	}
}

func execute(db *DbContext, command string) {
	switch command {
	case ".dbinfo":
		db.PrintDbInfo(os.Stdout)
	case ".tables":
		db.PrintTables(os.Stdout)
	case ".indexes":
		db.PrintIndexes(os.Stdout)
	case ".schema":
		db.PrintSchema(os.Stdout)
	default:
		if strings.Contains(strings.ToUpper(command), "SELECT") {
			db.HandleSelect(command, os.Stdout)
		} else {
			fmt.Println("Unknown command", command)
			os.Exit(1)
		}
	}
}

func repl(db *DbContext) {
	scanner := bufio.NewScanner(os.Stdin)
	fmt.Print("> ")
	for scanner.Scan() {
		command := strings.TrimSpace(scanner.Text())
		if command == ".exit" {
			break
		}
		execute(db, command)
		fmt.Print("> ")
	}
}
