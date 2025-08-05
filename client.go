package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"time"
)

const (
	// TOMBSTONE is a special value to mark a key as deleted.
	TOMBSTONE = "__TOMBSTONE__"
)

// ValueEntry holds the data and metadata for each key.
type ValueEntry struct {
	Value     string    `json:"value"`
	Timestamp time.Time `json:"timestamp"`
}

// SyncPayload is used to send updates between servers.
type SyncPayload struct {
	Key   string
	Entry ValueEntry
}

// Global variables
var (
	dataStore       = make(map[string]ValueEntry)
	mu              sync.RWMutex
	peerAddress     string
	myAddress       string
	persistenceFile string
)

// generateFilename creates a unique, safe filename from the server's address.
func generateFilename(address string) string {
	safeAddr := strings.ReplaceAll(address, ":", "_")
	return fmt.Sprintf("db_%s.json", safeAddr)
}

// saveDataToFile serializes the current dataStore to a JSON file.
func saveDataToFile() {
	mu.RLock()
	data, err := json.MarshalIndent(dataStore, "", "  ")
	mu.RUnlock()

	if err != nil {
		log.Printf("Error marshaling data for persistence: %v", err)
		return
	}
	if err := os.WriteFile(persistenceFile, data, 0644); err != nil {
		log.Printf("Error saving data to file %s: %v", persistenceFile, err)
	}
}

// loadDataFromFile loads the server's state from its JSON file.
func loadDataFromFile() (bool, time.Time) {
	data, err := os.ReadFile(persistenceFile)
	if err != nil {
		if os.IsNotExist(err) {
			log.Printf("No persistence file found. Will attempt full peer sync.")
		} else {
			log.Printf("Error reading persistence file %s: %v", persistenceFile, err)
		}
		return false, time.Time{}
	}

	mu.Lock()
	defer mu.Unlock()
	if err := json.Unmarshal(data, &dataStore); err != nil {
		log.Printf("Error parsing data from persistence file %s: %v", persistenceFile, err)
		return false, time.Time{}
	}

	// Find the latest timestamp from the loaded data
	var latestTimestamp time.Time
	for _, entry := range dataStore {
		if entry.Timestamp.After(latestTimestamp) {
			latestTimestamp = entry.Timestamp
		}
	}

	log.Printf("Successfully loaded %d keys from %s. Latest update at: %s", len(dataStore), persistenceFile, latestTimestamp.Format(time.RFC3339Nano))
	return true, latestTimestamp
}

// sendSync sends a single key-value update to the peer.
func sendSync(payload SyncPayload) {
	conn, err := net.DialTimeout("tcp", peerAddress, 2*time.Second)
	if err != nil {
		log.Printf("Failed to connect to peer for sync: %v", err)
		return
	}
	defer conn.Close()

	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		log.Printf("Failed to marshal sync payload: %v", err)
		return
	}

	syncCmd := fmt.Sprintf("SYNC %s\n", string(payloadBytes))
	if _, err := conn.Write([]byte(syncCmd)); err != nil {
		log.Printf("Error sending sync command to peer: %v", err)
	}
}

// requestDeltaSync asks the peer for all changes since the given timestamp.
func requestDeltaSync(since time.Time) {
	log.Printf("Requesting delta sync for changes since %s", since.Format(time.RFC3339Nano))
	conn, err := net.DialTimeout("tcp", peerAddress, 2*time.Second)
	if err != nil {
		log.Printf("Peer appears to be offline during delta sync. Error: %v", err)
		return
	}
	defer conn.Close()

	// 1. Send the request for a delta sync.
	cmd := fmt.Sprintf("SYNC_SINCE %s\n", since.Format(time.RFC3339Nano))
	if _, err := conn.Write([]byte(cmd)); err != nil {
		log.Printf("Failed to send delta sync request: %v", err)
		return
	}

	// 2. Read the JSON array of updates from the peer.
	data, err := bufio.NewReader(conn).ReadBytes('\n')
	if err != nil {
		log.Printf("Failed to read delta sync data from peer: %v", err)
		return
	}

	// 3. Unmarshal and apply the updates.
	var updates []SyncPayload
	if err := json.Unmarshal(data, &updates); err != nil {
		log.Printf("Failed to parse delta sync data from peer: %v", err)
		return
	}

	if len(updates) == 0 {
		log.Println("No new changes to sync from peer.")
		return
	}

	mu.Lock()
	for _, update := range updates {
		existingEntry, ok := dataStore[update.Key]
		if !ok || update.Entry.Timestamp.After(existingEntry.Timestamp) {
			dataStore[update.Key] = update.Entry
		}
	}
	mu.Unlock()

	log.Printf("Successfully applied %d updates from peer.", len(updates))
	go saveDataToFile()
}

// requestFullSync performs a one-time full data dump from the peer. Used on first start.
func requestFullSync() {
	log.Println("Attempting to perform FULL sync from peer...")
	conn, err := net.DialTimeout("tcp", peerAddress, 2*time.Second)
	if err != nil {
		log.Printf("Peer appears to be offline. Starting with a clean state. Error: %v", err)
		return
	}
	defer conn.Close()

	if _, err := conn.Write([]byte("FULL_SYNC_REQUEST\n")); err != nil {
		log.Printf("Failed to send sync request to peer: %v", err)
		return
	}
	data, err := bufio.NewReader(conn).ReadBytes('\n')
	if err != nil {
		log.Printf("Failed to read sync data from peer: %v", err)
		return
	}
	mu.Lock()
	defer mu.Unlock()
	if err := json.Unmarshal(data, &dataStore); err != nil {
		log.Printf("Failed to parse sync data from peer : %v", err)
		return
	}
	log.Printf("Successfully synchronized %d keys from peer %s", len(dataStore), peerAddress)
	go saveDataToFile()
}

func handleConnection(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)

	// Add this infinite loop to handle multiple commands
	for {
		message, err := reader.ReadString('\n')
		if err != nil {
			// If the client disconnects (EOF), we can just exit gracefully.
			// Otherwise, log the error.
			if err.Error() != "EOF" {
				log.Printf("Error reading from client %s: %v", conn.RemoteAddr(), err)
			}
			return // Exit the loop and close the connection
		}

		parts := strings.Fields(message)
		if len(parts) == 0 {
			continue // If the client sends an empty line, wait for the next command
		}

		cmd := strings.ToUpper(parts[0])

		switch cmd {
		case "SET":
			if len(parts) != 3 {
				conn.Write([]byte("ERROR: SET requires a key and a value.\n"))
				continue // Continue to the next command
			}
			key, value := parts[1], parts[2]
			entry := ValueEntry{Value: value, Timestamp: time.Now().UTC()}
			mu.Lock()
			dataStore[key] = entry
			mu.Unlock()

			// --- THIS IS THE NEW LINE ---
			log.Printf("SET key='%s' value='%s' from client %s", key, value, conn.RemoteAddr().String())

			conn.Write([]byte("OK\n"))
			go saveDataToFile()
			go sendSync(SyncPayload{Key: key, Entry: entry})

		case "GET":
			if len(parts) != 2 {
				conn.Write([]byte("ERROR: GET requires a key.\n"))
				continue
			}
			key := parts[1]
			mu.RLock()
			entry, ok := dataStore[key]
			mu.RUnlock()
			if ok && entry.Value != TOMBSTONE {
				conn.Write([]byte(fmt.Sprintf("%s\n", entry.Value)))
			} else {
				conn.Write([]byte("Key not found\n"))
			}

		case "DELETE":
			if len(parts) != 2 {
				conn.Write([]byte("ERROR: DELETE requires a key.\n"))
				continue
			}
			key := parts[1]
			entry := ValueEntry{Value: TOMBSTONE, Timestamp: time.Now().UTC()}
			mu.Lock()
			dataStore[key] = entry
			mu.Unlock()
			log.Printf("DELETE key='%s' from client %s", key, conn.RemoteAddr().String()) // Also good to log deletes
			conn.Write([]byte("OK\n"))
			go saveDataToFile()
			go sendSync(SyncPayload{Key: key, Entry: entry})

		// The logic for SYNC, SYNC_SINCE, and FULL_SYNC_REQUEST can stay the same
		// as they are typically single-transaction connections from the other server.
		// However, for robustness, we can just return after they are done.
		case "SYNC":
			var payload SyncPayload
			if err := json.Unmarshal([]byte(strings.Join(parts[1:], " ")), &payload); err != nil {
				log.Printf("Failed to parse SYNC payload: %v", err)
				return // This is a server-to-server command, close on finish
			}
			mu.Lock()
			existing, ok := dataStore[payload.Key]
			if !ok || payload.Entry.Timestamp.After(existing.Timestamp) {
				dataStore[payload.Key] = payload.Entry
				log.Printf("SYNC applied for key '%s'", payload.Key)
				go saveDataToFile()
			}
			mu.Unlock()
			return // End connection after sync

		case "SYNC_SINCE":
			since, err := time.Parse(time.RFC3339Nano, parts[1])
			if err != nil {
				log.Printf("Invalid timestamp format for SYNC_SINCE: %v", err)
				return
			}
			var updates []SyncPayload
			mu.RLock()
			for key, entry := range dataStore {
				if entry.Timestamp.After(since) {
					updates = append(updates, SyncPayload{Key: key, Entry: entry})
				}
			}
			mu.RUnlock()
			data, _ := json.Marshal(updates)
			conn.Write(data)
			conn.Write([]byte("\n"))
			return // End connection after sync

		case "FULL_SYNC_REQUEST":
			mu.RLock()
			data, _ := json.Marshal(dataStore)
			mu.RUnlock()
			conn.Write(data)
			conn.Write([]byte("\n"))
			return // End connection after sync

		default:
			conn.Write([]byte("ERROR: Invalid command.\n"))
		}
	}
}

func main() {
	if len(os.Args) != 3 {
		log.Fatalf("Usage: go run server.go <my_address> <peer_address>")
	}
	myAddress = os.Args[1]
	peerAddress = os.Args[2]
	persistenceFile = generateFilename(myAddress)

	// Startup sequence
	dataLoaded, latestTimestamp := loadDataFromFile()

	listener, err := net.Listen("tcp", myAddress)
	if err != nil {
		log.Fatalf("Error listening on %s: %v\n", myAddress, err)
	}
	defer listener.Close()

	log.Printf("Server (%s) started and listening...", myAddress)

	if dataLoaded {
		// If we loaded data, perform a delta sync to catch up
		go requestDeltaSync(latestTimestamp)
	} else {
		// If we have no data, perform a full sync
		go requestFullSync()
	}

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Error accepting connection: %v\n", err)
			continue
		}
		go handleConnection(conn)
	}
}
