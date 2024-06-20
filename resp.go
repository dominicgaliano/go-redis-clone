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
	v.array = make([]Value, 0)
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

// Marshall converts a parsed RESP value into a series of bytes representing
// the RESP response
func (v Value) Marshal() []byte {
	switch v.typ {
	case "string":
		return v.marshalString()
	case "bulk":
		return v.marshalBulk()
	case "array":
		return v.marshalArray()
	case "null":
		return v.marshalNull()
	case "error":
		return v.marshalError()
	default:
		return []byte{}
	}
}

func (v Value) marshalString() []byte {
	var bytes []byte

	bytes = append(bytes, STRING) // '+'
	bytes = append(bytes, v.str...)
	bytes = append(bytes, '\r', '\n')

	return bytes
}

func (v Value) marshalBulk() []byte {
	var bytes []byte

	bytes = append(bytes, BULK)                         // '$'
	bytes = append(bytes, strconv.Itoa(len(v.bulk))...) // bulk length ex. 12
	bytes = append(bytes, '\r', '\n')
	bytes = append(bytes, v.bulk...)
	bytes = append(bytes, '\r', '\n')

	return bytes
}

func (v Value) marshalArray() []byte {
    len := len(v.array)
	var bytes []byte

	bytes = append(bytes, ARRAY)                         // '*'
	bytes = append(bytes, strconv.Itoa(len)...) // array length ex. 3
	bytes = append(bytes, '\r', '\n')

    for i := 0; i < len; i++ {
		bytes = append(bytes, v.array[i].Marshal()...)
    }

    // // only add trailing \r\n if array not empty
    // if len != 0 {
    //     bytes = append(bytes, '\r', '\n') 
    // }

	return bytes
}

func (v Value) marshalNull() []byte {
	// utilizes the RESP2 form for NULL values, for more info see:
	// https://redis.io/docs/latest/develop/reference/protocol-spec/#arrays
	return []byte("$-1\r\n")
}

func (v Value) marshalError() []byte {
	var bytes []byte

	bytes = append(bytes, ERROR)    // '-'
	bytes = append(bytes, v.str...) // error saved in string
	bytes = append(bytes, '\r', '\n')

	return bytes
}

// Writer struct is used to output the RESP response
type Writer struct {
	writer io.Writer
}

func NewWriter(w io.Writer) *Writer {
	return &Writer{writer: w}
}

func (w *Writer) Write(v Value) error {
	bytes := v.Marshal()

	_, err := w.writer.Write(bytes)
	if err != nil {
		return err
	}

	return nil
}
