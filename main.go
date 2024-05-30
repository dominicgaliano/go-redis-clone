package main

import (
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
)

func main() {
	PORT := 6379
	fmt.Printf("Server listening on port %d\n", PORT)

	// Create a TCP listener
	l, err := net.Listen("tcp", ":"+strconv.Itoa(PORT))
	if err != nil {
		fmt.Println(err)
		return
	}

	// Receive requests
	conn, err := l.Accept()
	if err != nil {
		fmt.Println(err)
		return
	}

	defer conn.Close() // close connection once finished

	// wait for client requests and respond
	for {
		buf := make([]byte, 1024)

		// read messages from client
		_, err := conn.Read(buf)
		if err != nil {
			if err == io.EOF {
				break
			}
			fmt.Println("error reading from client: ", err.Error())
			os.Exit(1)
		}

		// for now, ignore request and send back PONG
		conn.Write([]byte("+OK\r\n"))
	}
}
