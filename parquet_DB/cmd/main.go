package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

func main() {
	fmt.Println("Welcome to NoelDB!")
	fmt.Println("Type SQLcommands (e.g. CREATE, INSERT,SELECT) or EXIT to quit")
	fmt.Println("--------------------------------------------------------------------")

	reader := bufio.NewReader(os.Stdin)

	for {
		fmt.Print("noel> ")

		input, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("Error reading input:", err)
			continue
		}

		command := strings.TrimSpace(input)

		switch strings.ToUpper(command) {

		case "EXIT":
			fmt.Println("Exiting NoelDB. Goodbye!")
			os.Exit(0)
		case "":
			continue
		default:
			fmt.Printf("Received command: %s\n", command)
		}

	}
}
