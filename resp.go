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
	valueType string
	str       string
	num       int
	bulk      string
	array     []Value
}

// RespParser represents a RESP protocol parser
type RespParser struct {
	reader *bufio.Reader
}

// struct to take the io.Writer
type Writer struct {
	writer io.Writer
}

// PUBLIC
// Creates new RESP parser
func NewResp(rd io.Reader) *RespParser {
	return &RespParser{reader: bufio.NewReader(rd)}
}

func NewWriter(w io.Writer) *Writer {
	return &Writer{writer: w}
}

// 'Marshal' or serialize the data into RESP format
func (v Value) Marshal() []byte {
	switch v.valueType {
	case "array":
		return v.marshalArray()
	case "bulk":
		return v.marshalBulk()
	case "error":
		return v.marshalError()
	case "integer":
		return v.marshalInteger()
	case "null":
		return v.marshalNull()
	case "string":
		return v.marshalString()
	default:
		return []byte{}
	}
}

// parses the next value from RESP stream
func (r *RespParser) Read() (Value, error) {
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

// Take a value and write the RESP formatted version to writer
func (w *Writer) Write(v Value) error {
	var bytes = v.Marshal()
	_, err := w.writer.Write(bytes)

	return err
}

// PRIVATE
// marshal an array into RESP format
func (v Value) marshalArray() []byte {
	arrayLength := len(v.array)
	var bytes []byte
	bytes = append(bytes, ARRAY)
	bytes = append(bytes, strconv.Itoa(arrayLength)...)
	bytes = append(bytes, '\r', '\n')

	// recursively marshal contents of the array
	for i := 0; i < arrayLength; i++ {
		bytes = append(bytes, v.array[i].Marshal()...)
	}

	return bytes
}

// marshal a bulk string into RESP format
func (v Value) marshalBulk() []byte {
	var bytes []byte
	bytes = append(bytes, BULK)
	bytes = append(bytes, strconv.Itoa(len(v.bulk))...)
	bytes = append(bytes, '\r', '\n')
	bytes = append(bytes, v.bulk...)
	bytes = append(bytes, '\r', '\n')

	return bytes
}

// marshal an error into RESP format
func (v Value) marshalError() []byte {
	var bytes []byte
	bytes = append(bytes, ERROR)
	bytes = append(bytes, v.str...)
	bytes = append(bytes, '\r', '\n')

	return bytes
}

// marshal in int into RESP format
func (v Value) marshalInteger() []byte {
	valueStr := strconv.Itoa(v.num)

	var bytes []byte
	bytes = append(bytes, INTEGER)
	bytes = append(bytes, valueStr...)
	bytes = append(bytes, '\r', '\n')

	return bytes
}

// marshal a null
func (v Value) marshalNull() []byte {
	return []byte("$-1\r\n")
}

// marshal a string into RESP format
func (v Value) marshalString() []byte {
	var bytes []byte
	bytes = append(bytes, STRING)
	bytes = append(bytes, v.str...)
	bytes = append(bytes, '\r', '\n')

	return bytes
}

// reads an array value from the RESP stream
func (r *RespParser) readArray() (Value, error) {
	v := Value{}
	v.valueType = "array"

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
func (r *RespParser) readBulk() (Value, error) {
	v := Value{}
	v.valueType = "bulk"

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
func (r *RespParser) readInteger() (x int, n int, err error) {
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
func (r *RespParser) readLine() (line []byte, n int, err error) {
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
