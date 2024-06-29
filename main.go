package main

import (
	"fmt"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
)

func main() {
	// Create server
	listener, err := net.Listen("tcp", ":6379")
	if err != nil {
		fmt.Println("Listener creation error: ", err.Error())
		os.Exit(1)
	}
	fmt.Println("Listening on port :6379")

	// Create AppendOnlyFile for persistence
	appOnlyFile, err := NewAof("database.aof")
	if err != nil {
		fmt.Println("AppendOnlyFile creation error: ", err.Error())
		os.Exit(1)
	}

	// Handle termination signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// WaitGroup to wait for all go routines to finish
	var wg sync.WaitGroup
	var shuttingDown bool

	go func() {
		<-sigChan
		fmt.Println("\nTermination signal received. Shutting down")

		// Indicate the server is shutting down
		shuttingDown = true

		// Get current state and rewrite AOF
		currentState := GetCurrentState()
		if err := appOnlyFile.rewriteAof(currentState); err != nil {
			fmt.Println("Error rewriting AOF:", err)
		}

		// Close listener and file
		listener.Close()
		appOnlyFile.CloseFile()
		wg.Wait() // Wait for all goroutines to finish
		os.Exit(0)
	}()

	for {
		if shuttingDown {
			break
		}

		// Accept new connections
		connection, err := listener.Accept()
		if err != nil {
			if shuttingDown {
				// If we are shutting down, exit the loop
				fmt.Println("Shutting down listener")
				break
			} else {
				fmt.Println("Connection acceptance error:", err.Error())
				continue
			}
		}

		// Increment the WaitGroup counter
		wg.Add(1)

		// Handle each connection in a new go routine
		go func(conn net.Conn) {
			defer wg.Done() // Decrement the counter when the go routine completes
			handleConnection(conn, appOnlyFile)
		}(connection)
	}
}

func handleConnection(
	connection net.Conn,
	appOnlyFile *AppendOnlyFile) {
	defer connection.Close()

	for {
		resp := NewResp(connection)
		if resp == nil {
			fmt.Println("NewResp returned nil")
			return
		}

		value, err := resp.Read()
		if err != nil {
			fmt.Println("RESP read error:", err.Error())
			return
		}

		if value.valueType != "array" {
			fmt.Println("Invalid request, array expected")
			continue
		}

		if len(value.array) == 0 {
			fmt.Println("Invalid request, array length must be > 0")
			continue
		}

		command := strings.ToUpper(value.array[0].bulk)
		args := value.array[1:]

		writer := NewWriter(connection)
		if writer == nil {
			fmt.Println("NewWriter returned nil")
			return
		}

		handler, ok := Handlers[command]

		if !ok {
			fmt.Println("Invalid command: ", command)
			writer.Write(Value{
				valueType: ValueTypeString,
				str:       "",
			})
			continue
		}

		if logCommands[command] {
			err = appOnlyFile.Write(value)
			if err != nil {
				fmt.Println("AppendOnlyFile write error:", err.Error())
				continue
			}
		}

		result := handler(args)
		writer.Write(result)
	}
}
