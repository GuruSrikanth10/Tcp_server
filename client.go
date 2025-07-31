package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"time"
)

var (
	serverAddresses   = []string{"localhost:8080", "localhost:8081"}
	connectionTimeout = 3 * time.Second
)

// connectToServer attempts to establish a connection to an available server.
// It returns the connection, the address of the connected server, and any error.
func connectToServer() (net.Conn, string, error) {
	for _, addr := range serverAddresses {
		fmt.Printf("Attempting to connect to %s...\n", addr)
		conn, err := net.DialTimeout("tcp", addr, connectionTimeout)
		if err != nil {
			log.Printf("Failed to connect to %s: %v. Trying next server...", addr, err)
			continue
		}
		return conn, addr, nil
	}
	return nil, "", fmt.Errorf("could not connect to any server")
}

func main() {
	var conn net.Conn
	var addr string
	var err error

	// Attempt an initial connection.
	conn, addr, err = connectToServer()
	if err != nil {
		log.Fatalf("Failed to connect to any server on startup: %v", err)
	}
	defer conn.Close()

	fmt.Printf("Successfully connected to %s\n", addr)
	fmt.Println("You can now send commands to the server (e.g., SET name alice, GET name).")
	fmt.Println("Type 'EXIT' or 'QUIT' to close the connection.")

	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Printf("Client> ")
		input, _ := reader.ReadString('\n')
		input = strings.TrimSpace(input)

		if strings.ToUpper(input) == "EXIT" || strings.ToUpper(input) == "QUIT" {
			fmt.Println("Exiting client.")
			return
		}

		// Try to send the command and handle potential errors.
		_, err = conn.Write([]byte(input + "\n"))
		if err != nil {
			fmt.Printf("Connection to %s lost: %v. Attempting to reconnect...\n", addr, err)
			// Close the old connection and try to reconnect.
			conn.Close()
			conn, addr, err = connectToServer()
			if err != nil {
				log.Fatalf("Failed to reconnect to any server: %v", err)
			}
			fmt.Printf("Reconnected to %s. Please re-enter your command.\n", addr)
			continue
		}

		// Try to read the response and handle potential errors.
		serverResponse, err := bufio.NewReader(conn).ReadString('\n')
		if err != nil {
			fmt.Printf("Connection to %s lost: %v. Attempting to reconnect...\n", addr, err)
			// Close the old connection and try to reconnect.
			conn.Close()
			conn, addr, err = connectToServer()
			if err != nil {
				log.Fatalf("Failed to reconnect to any server: %v", err)
			}
			fmt.Printf("Reconnected to %s. Please re-enter your command.\n", addr)
			continue
		}

		fmt.Printf("Server Response: %s", serverResponse)
	}
}
