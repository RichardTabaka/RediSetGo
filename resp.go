package main

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
)

const (
	STRING  = '+'
	ERROR   = '-'
	INTEGER = ':'
	BULK    = '$'
	ARRAY   = '*'
)

type Value struct {
	typ   string
	str   string
	num   int
	bulk  string
	array []Value
}

// Resp represents a RESP protocol parser
type Resp struct {
	reader *bufio.Reader
}

// Creates new RESP parser
func NewResp(rd io.Reader) *Resp {
	return &Resp{reader: bufio.NewReader(rd)}
}

// parses the next value from RESP stream
func (r *Resp) Read() (Value, error) {
	valueType, err := r.reader.ReadByte()

	if err != nil {
		return Value{}, err
	}

	switch valueType {
	case ARRAY:
		return r.readArray()
	case BULK:
		return r.readBulk()
	default:
		fmt.Println("Unknown type: ", string(valueType))
		return Value{}, nil
	}
}

// reads an array value from the RESP stream
func (r *Resp) readArray() (Value, error) {
	v := Value{}
	v.typ = "array"

	// Get array length
	arrayLength, _, err := r.readInteger()
	if err != nil {
		return v, err
	}

	// parse and read each value in the array
	v.array = make([]Value, 0)
	for i := 0; i < arrayLength; i++ {
		val, err := r.Read()
		if err != nil {
			return v, err
		}
		v.array = append(v.array, val)
	}

	return v, nil
}

// reads a bulk string from the RESP stream
func (r *Resp) readBulk() (Value, error) {
	v := Value{}
	v.typ = "bulk"

	// get length of bulk string
	bulkLength, _, err := r.readInteger()
	if err != nil {
		return v, err
	}

	// read bulk string data
	bulk := make([]byte, bulkLength)
	_, err = io.ReadFull(r.reader, bulk)
	if err != nil {
		return v, err
	}

	v.bulk = string(bulk)

	// Consume the trailing \r\n
	_, _, err = r.readLine()
	if err != nil {
		return v, err
	}

	return v, nil
}

// reads an int value from the RESP stream
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

// reads a line ending with \r\n
func (r *Resp) readLine() (line []byte, n int, err error) {
	for {
		b, err := r.reader.ReadByte()
		if err != nil {
			return nil, 0, err
		}
		n += 1
		line = append(line, b)
		if len(line) >= 2 && line[len(line)-2] == '\r' {
			break
		}
	}
	return line[:len(line)-2], n, nil
}
