package main

import (
	"bufio"
	"io"
	"os"
	"sync"
	"time"
)

type AppendOnlyFile struct {
	file  *os.File
	rd    *bufio.Reader
	mutex sync.Mutex
}

var logCommands = map[string]bool{
	"GET":     false,
	"HGET":    false,
	"HGETALL": false,
	"HSET":    true,
	"PING":    false,
	"SET":     true,
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
