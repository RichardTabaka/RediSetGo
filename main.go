package main

import (
	"fmt"
	"net"
	"os"
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

		_ = value

		writer := NewWriter(connection)

		writer.Write(Value{valueType: "string", str: "OK"})
	}
}
