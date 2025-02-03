package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/rinimisini112/sqliter/internal/executor"
)

func main() {
	dbPath := "db.sqliter"

	fmt.Println("Welcome to SQLiter CLI.")
	fmt.Println("Enter your SQL commands (type 'exit' to quit):")

	scanner := bufio.NewScanner(os.Stdin)

	for {
		fmt.Print("> ")

		if !scanner.Scan() {
			break
		}

		input := scanner.Text()

		if strings.TrimSpace(strings.ToLower(input)) == "exit" {
			fmt.Println("Exiting SQLiter CLI.")
			break
		}

		if strings.TrimSpace(input) == "" {
			continue
		}

		err := executor.ExecuteSQL(dbPath, input)
		if err != nil {
			log.Printf("Error executing SQL: %v\n", err)
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatalf("Error reading standard input: %v", err)
	}
}
