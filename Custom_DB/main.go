package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

func main() {
	fmt.Println("Welcome to  CUSTOMDB!")
	fmt.Println("Type 'exit' or 'quit' to exit the shell")

	reader := bufio.NewReader(os.Stdin)

	for {

		fmt.Print("CustomDB> ")

		input, err := reader.ReadString('\n')

		if err != nil {
			fmt.Println("Error in input")
			continue
		}
		input = strings.TrimSpace(input)

		if input == "exit" || input == "quit" {
			fmt.Println("Closing the shell...")
			break
		}

		fmt.Println("Your typed: ", input)

	}
}
