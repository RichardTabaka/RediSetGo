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

	// Open the AOF file with truncate mode to clear it
	oldPath := appendOnlyFile.file.Name()
	aofFile, err := os.OpenFile(oldPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0666)
	if err != nil {
		fmt.Println("Error opening AOF file:", err)
		return err
	}

	// Write current state directly to the AOF file
	for key, value := range currentState {
		if value.valueType == ValueTypeArray && len(value.array) > 0 && value.array[0].bulk == "HSET" {
			// Write HSET command
			_, err := aofFile.Write(value.Marshal())
			if err != nil {
				fmt.Println("Error writing to AOF file:", err)
				aofFile.Close() // Ensure aofFile is closed in case of error
				return err
			}
		} else {
			// Write other commands
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
			}

			_, err := aofFile.Write(command.Marshal())
			if err != nil {
				fmt.Println("Error writing to AOF file:", err)
				aofFile.Close() // Ensure aofFile is closed in case of error
				return err
			}
		}
	}

	// Flush all writes and close the file
	if err := aofFile.Close(); err != nil {
		fmt.Println("Error closing AOF file:", err)
		return err
	}

	// Reopen the AOF file for appending
	appendOnlyFile.file, err = os.OpenFile(oldPath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		fmt.Println("Error reopening AOF file:", err)
		return err
	}

	appendOnlyFile.rd = bufio.NewReader(appendOnlyFile.file)
	fmt.Println("AOF rewrite completed successfully")

	return nil
}
