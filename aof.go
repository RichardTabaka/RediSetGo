package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"
)

type AppendOnlyFile struct {
	file  *os.File
	rd    *bufio.Reader
	mutex sync.Mutex
}

var logCommands = map[string]bool{
	"DEL":  true,
	"HDEL": true,
	"HSET": true,
	"SET":  true,

	"GET":     false,
	"HGET":    false,
	"HGETALL": false,
	"KEYS":    false,
	"PING":    false,
}

func NewAof(path string) (*AppendOnlyFile, error) {
	// create or open file for backup
	openFile, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}

	appendOnlyFile := &AppendOnlyFile{
		file: openFile,
		rd:   bufio.NewReader(openFile),
	}

	// load contents of file to memory
	appendOnlyFile.Read(func(value Value) {
		command := strings.ToUpper(value.array[0].bulk)
		args := value.array[1:]

		handler, ok := Handlers[command]
		if !ok {
			fmt.Println("Invalid command: ", command)
			return
		}
		handler(args)
	})

	// clean existing file
	currentState := GetCurrentState()
	appendOnlyFile.rewriteAof(currentState)

	// goroutine that will sync to disk every 5 seconds
	go func() {
		for {
			appendOnlyFile.mutex.Lock()
			appendOnlyFile.file.Sync()
			appendOnlyFile.mutex.Unlock()
			time.Sleep(time.Second * 5)
		}
	}()

	return appendOnlyFile, nil
}

// Method to safely close file on server shutdown
func (appendOnlyFile *AppendOnlyFile) CloseFile() error {
	appendOnlyFile.mutex.Lock()
	defer appendOnlyFile.mutex.Unlock()

	return appendOnlyFile.file.Close()
}

// Read contents of file into memory
func (appendOnlyFile *AppendOnlyFile) Read(fn func(value Value)) error {
	appendOnlyFile.mutex.Lock()
	defer appendOnlyFile.mutex.Unlock()

	appendOnlyFile.file.Seek(0, io.SeekStart)

	reader := NewResp(appendOnlyFile.file)

	for {
		value, err := reader.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		fn(value)
	}
	return nil
}

// Method to write commands to a file for persistence
func (appendOnlyFile *AppendOnlyFile) Write(value Value) error {
	appendOnlyFile.mutex.Lock()
	defer appendOnlyFile.mutex.Unlock()

	_, err := appendOnlyFile.file.Write(value.Marshal())

	return err
}

func (appendOnlyFile *AppendOnlyFile) rewriteAof(currentState map[string]Value) error {
	appendOnlyFile.mutex.Lock()
	defer appendOnlyFile.mutex.Unlock()

	// Create a temp file
	tempFile, err := os.CreateTemp("", "aof-rewrite.aof")
	if err != nil {
		fmt.Println("Error creating temp file:", err)
		return err
	}

	// Write current state to temp file
	for key, value := range currentState {
		var command Value
		if value.valueType == ValueTypeString {
			command = Value{
				valueType: ValueTypeArray,
				array: []Value{
					{valueType: ValueTypeBulk, bulk: "SET"},
					{valueType: ValueTypeBulk, bulk: key},
					{valueType: ValueTypeBulk, bulk: value.str},
				},
			}
		} else if value.valueType == ValueTypeBulk {
			command = Value{
				valueType: ValueTypeArray,
				array: []Value{
					{valueType: ValueTypeBulk, bulk: "HSET"},
					{valueType: ValueTypeBulk, bulk: key},
					{valueType: ValueTypeBulk, bulk: value.bulk},
				},
			}
		}

		_, err := tempFile.Write(command.Marshal())
		if err != nil {
			fmt.Println("Error writing to temp file:", err)
			tempFile.Close() // Ensure tempFile is closed in case of error
			return err
		}
	}

	// Close temp file to flush all writes
	if err := tempFile.Close(); err != nil {
		fmt.Println("Error closing temp file:", err)
		return err
	}

	// Open the new file to write into
	oldPath := appendOnlyFile.file.Name()
	newFile, err := os.OpenFile(oldPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0666)
	if err != nil {
		fmt.Println("Error opening new AOF file:", err)
		return err
	}

	// Open temp file again for reading
	tempFile, err = os.Open(tempFile.Name())
	if err != nil {
		fmt.Println("Error reopening temp file:", err)
		newFile.Close() // Ensure newFile is closed in case of error
		return err
	}
	defer tempFile.Close()
	defer newFile.Close()

	_, err = io.Copy(newFile, tempFile)
	if err != nil {
		fmt.Println("Error copying temp file to AOF file:", err)
		return err
	}

	return nil
}
