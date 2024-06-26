package main

import (
	"fmt"
	"net"
	"os"
	"strings"
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
	defer appOnlyFile.CloseFile()

	// load contents of file to memory
	appOnlyFile.Read(func(value Value) {
		command := strings.ToUpper(value.array[0].bulk)
		args := value.array[1:]

		handler, ok := Handlers[command]
		if !ok {
			fmt.Println("Invalid command: ", command)
			return
		}
		handler(args)
	})

	for {
		// Accept new connections
		connection, err := listener.Accept()
		if err != nil {
			fmt.Println("Connection acceptance error: ", err.Error())
			continue
		}

		// Handle each connection in a new goroutine
		go handleConnection(connection, appOnlyFile)
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
			fmt.Println("RESP read error: ", err.Error())
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
				fmt.Println("AppendOnlyFile write error: ", err.Error())
				continue
			}
		}

		result := handler(args)
		writer.Write(result)
	}
}
