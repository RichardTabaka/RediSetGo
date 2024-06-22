package main

import (
	"fmt"
	"io"
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
		buffer := make([]byte, 1024)

		_, err = connection.Read(buffer)
		if err != nil {
			if err == io.EOF {
				break
			}
			fmt.Println("Client read error: ", err.Error())
			os.Exit(1)
		}

		// Ignore request for now, send back PONG
		connection.Write([]byte("+OK\r\n"))
	}
}
