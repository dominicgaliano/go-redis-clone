package main

import (
	"bufio"
	"io"
	"os"
	"sync"
	"time"
)

// holds the read only file stored on disk
type Aof struct {
	file *os.File
	rd   *bufio.Reader
	mu   sync.Mutex
}

// reads AOF at path if it exists, otherwise generates a new AOF at path 
// starts automatic syncing to disk for AOF
func NewAof(path string) (*Aof, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}

	aof := &Aof{
		file: f,
		rd:   bufio.NewReader(f),
	}

    // Start a goroutine to sync AOF with disk every 1 second
    go func() {
        for {
            aof.mu.Lock()
            aof.file.Sync()
            aof.mu.Unlock()
            time.Sleep(time.Second)
        }
    }()

    return aof, nil
}

// properly closes AOF
func (aof *Aof) Close() error {
    aof.mu.Lock()
    defer aof.mu.Unlock()

    return aof.file.Close()
}

// Write command to file
func (aof *Aof) Write(value Value) error {
    aof.mu.Lock()
    defer aof.mu.Unlock()

    _, err := aof.file.Write(value.Marshal())
    if err != nil {
        return err
    }

    return nil
}

// Read file contents to local memory
func (aof *Aof) Read(fn func(value Value)) error {
    aof.mu.Lock()
    defer aof.mu.Unlock()

    aof.file.Seek(0, io.SeekStart)

    reader := NewResp(aof.file)

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
