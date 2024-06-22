package main

import (
	"fmt"
	"net"
	"os"
	"strings"
)

func main() {
	fmt.Println("Listening on port :6379")

	// Create server
	listener, err := net.Listen("tcp", ":6379")
	if err != nil {
		fmt.Println("Listener creation error: ", err.Error())
		os.Exit(1)
	}

	// Listen for connections
	connection, err := listener.Accept()
	if err != nil {
		fmt.Println("Connection acceptance error: ", err.Error())
	}

	defer connection.Close()

	for {
		resp := NewResp(connection)
		value, err := resp.Read()
		if err != nil {
			fmt.Println("RESP read error: ", err.Error())
			os.Exit(1)
		}

		if value.valueType != "array" {
			fmt.Println("Invalid request, expected array")
			continue
		}

		if len(value.array) == 0 {
			fmt.Println("Invalid request, array length must be > 0")
			continue
		}

		command := strings.ToUpper(value.array[0].bulk)
		args := value.array[1:]

		writer := NewWriter(connection)

		handler, ok := Handlers[command]

		if !ok {
			fmt.Println("Invalid command: ", command)
			writer.Write(Value{valueType: "string", str: ""})
			continue
		}

		result := handler(args)
		writer.Write(result)
	}
}
