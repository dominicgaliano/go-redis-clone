package main

import (
	"fmt"
	"net"
	"strconv"
	"strings"
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

        if value.typ != "array" {
            fmt.Println("Invalid request, expected array")
            continue
        }

        if len(value.array) == 0 {
            fmt.Println("Invalid request, expected array length > 0")
            continue
        }

        command := strings.ToUpper(value.array[0].bulk)
        args := value.array[1:]

		writer := NewWriter(conn)

        handler, ok := Handlers[command]
        if !ok {
            fmt.Println("Invalid command: ", command)
            writer.Write(Value{typ: "string", str: ""})
            continue
        }

        result := handler(args)
        writer.Write(result)
	}
}
