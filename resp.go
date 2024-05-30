package main

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
)

// RESP DATA TYPE SYMBOLS
const (
	STRING   = '+'
	ERROR    = '-'
	INTERGER = ':'
	BULK     = '$'
	ARRAY    = '*'
)

// used to parse/deserialize RESP commands
type Value struct {
	typ   string
	str   string
	num   int
	bulk  string
	array []Value
}

// Resp is a wrapper around a reader used to parse Resp commands
type Resp struct {
    reader *bufio.Reader
}

// NewResp returns a pointer to a new Resp instance using the provided reader, rd.
func NewResp(rd io.Reader) *Resp {
    return &Resp{reader: bufio.NewReader(rd)}
}

// readLine reads bytes from the reader for a single line (delimited by \r\n) 
// and returns a slice of all bytes (excluding \r\n), the number of bytes read
// (including \r\n), and optionally and error.
func (r *Resp) readLine() (line []byte, n int, err error) {
    // read bytes until \r\n encountered
    for {
        b, err := r.reader.ReadByte()
        if err != nil {
            return nil, 0, err
        }
        n += 1
        line = append(line, b)
    
        // break once \r\n found
        if len(line) >= 2 && line[len(line)-2] == '\r' && line[len(line)-1] == '\n' {
            break
        }
    }

    // return line without \r\n
    return line[:len(line)-2], n, nil
}

// readInteger reads a line of a RESP command and returns the integer value of
// the line and number of bytes read if successful otherwise returns an error.
func (r *Resp) readInteger() (x int, n int, err error) {
    line, n, err := r.readLine()
    if err != nil {
        return 0, 0, err
    }

	i64, err := strconv.ParseInt(string(line), 10, 64)
    if err != nil {
        return 0, n, err
    }

    return int(i64), n, nil
}

// Read parses the RESP command and returns the appropriate value if successful
// otherwise it returns an error.
func (r *Resp) Read() (Value, error) {
    _type, err := r.reader.ReadByte()
    if err != nil {
        return Value{}, err
    }

    switch _type {
    case ARRAY:
        return r.readArray()
    case BULK:
        return r.readBulk()
    default:
        fmt.Printf("Unknown type: %v", string(_type))
        return Value{}, err
    }
}

// readArray recursively parses RESP commands of type array
func (r *Resp) readArray() (Value, error) {
    v := Value{}
    v.typ = "array"

    // read length of array
    length, _, err := r.readInteger()
    if err != nil {
        return v, err
    }

    // recursively parse and read each value in array
    v.array = make([]Value, length)
    for i := 0; i < length; i++ {
        val, err := r.Read()
        if err != nil {
            return v, err
        }

        // append parsed value
        v.array = append(v.array, val)
    }

    return v, nil
}

// readBulk parses RESP commands of type bulk
func (r *Resp) readBulk() (Value, error) {
    v := Value{}
    v.typ = "bulk"

    // read length of array
    length, _, err := r.readInteger()
    if err != nil {
        return v, err
    }

    bulk := make([]byte, length)

    _, err = r.reader.Read(bulk)
    if err != nil {
        return v, err
    }

    v.bulk = string(bulk)

    // read trailing \r\n
    r.readLine()

    return v, nil
}

