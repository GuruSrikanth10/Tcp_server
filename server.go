package main

import (
	"bufio"
	"encoding/json" // For serializing the data store
	"fmt"
	"log"
	"math/rand" // For preventing startup race conditions
	"net"
	"os"
	"strings"
	"sync"
	"time"
)

// dataStore is a simple in-memory key-value store.
var dataStore = make(map[string]string)

// mu is a mutex to protect the dataStore from concurrent access.
var mu sync.RWMutex

var peerAddress string
var myAddress string

// sendSync sends a synchronization command for a single key-value pair to the peer server.
func sendSync(key, value string) {
	conn, err := net.DialTimeout("tcp", peerAddress, 2*time.Second)
	if err != nil {
		log.Printf("Failed to connect to peer for sync: %v", err)
		return
	}
	defer conn.Close()

	syncCmd := fmt.Sprintf("SYNC %s %s\n", key, value)
	_, err = conn.Write([]byte(syncCmd))
	if err != nil {
		log.Printf("Error sending sync command to peer: %v", err)
	}
}

// requestInitialSync is called on server startup. It attempts to fetch the entire dataset from its peer.
func requestInitialSync() {
	// Add a small random delay to prevent a race condition where both servers start simultaneously
	// and try to sync from each other, leading to a deadlock.
	time.Sleep(time.Duration(rand.Intn(1000)+200) * time.Millisecond)
	log.Println("Attempting to perform initial sync from peer...")

	conn, err := net.DialTimeout("tcp", peerAddress, 2*time.Second)
	if err != nil {
		log.Printf("Peer %s appears to be offline. Starting with a clean state. Error: %v", peerAddress, err)
		return
	}
	defer conn.Close()

	// 1. Send the request for a full data dump.
	_, err = conn.Write([]byte("FULL_SYNC_REQUEST\n"))
	if err != nil {
		log.Printf("Failed to send sync request to peer: %v", err)
		return
	}

	// 2. Read the JSON data dump from the peer.
	data, err := bufio.NewReader(conn).ReadBytes('\n')
	if err != nil {
		log.Printf("Failed to read sync data from peer: %v", err)
		return
	}

	// 3. Unmarshal the data and update the local dataStore.
	var peerDataStore map[string]string
	if err := json.Unmarshal(data, &peerDataStore); err != nil {
		log.Printf("Failed to parse sync data from peer : %v", err)
		return
	}

	mu.Lock()
	dataStore = peerDataStore
	mu.Unlock()

	log.Printf(" Successfully synchronized %d keys from peer %s", len(peerDataStore), peerAddress)
}

func handleConnection(conn net.Conn, serverName string) {
	defer conn.Close()

	remoteAddr := conn.RemoteAddr().String()
	fmt.Printf("[%s] New connection from: %s\n", serverName, remoteAddr)

	reader := bufio.NewReader(conn)
	// We read only the first line for the initial command check.
	// For regular commands, we'll loop. For sync, we handle and close.
	message, err := reader.ReadString('\n')
	if err != nil {
		if err.Error() != "EOF" { // Don't log EOF as a critical error
			log.Printf("[%s] Connection from %s closed before command read: %v\n", serverName, remoteAddr, err)
		}
		return
	}

	command := strings.Fields(strings.TrimSpace(message))
	if len(command) == 0 {
		return
	}

	var response string
	cmdType := strings.ToUpper(command[0])

	// A special case for the one-off full sync request.
	if cmdType == "FULL_SYNC_REQUEST" {
		fmt.Printf("[%s] Received FULL_SYNC_REQUEST from peer.\n", serverName)
		mu.RLock()
		jsonData, err := json.Marshal(dataStore)
		mu.RUnlock()

		if err != nil {
			log.Printf("[%s] Error marshaling data for sync: %v", serverName, err)
			return // Close connection on error
		}

		conn.Write(jsonData)
		conn.Write([]byte("\n")) // Use newline as a delimiter
		fmt.Printf("[%s] Sent full data dump to peer.\n", serverName)
		return // This is a one-off transaction, so close the connection.
	}

	// For all other commands, enter the persistent connection loop.
	// We start by processing the command we already read.
	for {
		switch cmdType {
		case "SET":
			if len(command) != 3 {
				response = "ERROR: SET command requires a key and a value.\n"
			} else {
				key, value := command[1], command[2]
				mu.Lock()
				dataStore[key] = value
				mu.Unlock()
				response = "OK\n"
				fmt.Printf("[%s] Client SET key='%s' value='%s'\n", serverName, key, value)
				go sendSync(key, value)
			}
		case "GET":
			if len(command) != 2 {
				response = "ERROR: GET command requires a key.\n"
			} else {
				key := command[1]
				mu.RLock()
				value, ok := dataStore[key]
				mu.RUnlock()
				if ok {
					response = fmt.Sprintf("%s\n", value)
				} else {
					response = "Key not found\n"
				}
			}
		case "SYNC":
			if len(command) != 3 {
				response = "ERROR: SYNC command requires a key and a value.\n"
			} else {
				key, value := command[1], command[2]
				mu.Lock()
				dataStore[key] = value
				mu.Unlock()
				response = "OK\n"
				fmt.Printf("[%s] Received SYNC from peer: key='%s' value='%s'\n", serverName, key, value)
			}
		default:
			response = "ERROR: Invalid command. Use SET or GET.\n"
		}

		_, err = conn.Write([]byte(response))
		if err != nil {
			log.Printf("[%s] Error writing to %s: %v\n", serverName, remoteAddr, err)
			return
		}

		// Wait for the next command from the same client
		message, err = reader.ReadString('\n')
		if err != nil {

			log.Printf("[%s] Connection closed by %s: %v\n", serverName, remoteAddr, err)
			return
		}
		command = strings.Fields(strings.TrimSpace(message))
		if len(command) == 0 {
			continue
		}
		cmdType = strings.ToUpper(command[0])
	}
}

func main() {
	if len(os.Args) != 3 {
		log.Fatalf("Usage: go run server.go <my_address> <peer_address>")
	}
	myAddress = os.Args[1]
	peerAddress = os.Args[2]
	serverName := fmt.Sprintf("Server (%s)", myAddress)

	// Seed the random number generator
	rand.New(rand.NewSource(time.Now().UnixNano()))

	listener, err := net.Listen("tcp", myAddress)
	if err != nil {
		log.Fatalf("[%s] Error listening on %s: %v\n", serverName, myAddress, err)
	}
	defer listener.Close()

	fmt.Printf("[%s] TCP server started and listening...\n", serverName)

	// Asynchronously perform initial data sync from the peer when starting up.
	go requestInitialSync()

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("[%s] Error accepting connection: %v\n", serverName, err)
			continue
		}
		go handleConnection(conn, serverName)
	}
}
