package main

import (
	"fmt"
	"net"
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
        resp := NewResp(conn)
        value, err := resp.Read()
        if err != nil {
            fmt.Println(err)
            return
        }

        fmt.Println(value)

		// for now, ignore request and send back PONG
		conn.Write([]byte("+OK\r\n"))
	}
}
