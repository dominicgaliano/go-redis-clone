package main

import (
	"fmt"
	"sync"
)

var Handlers = map[string]func([]Value) Value{
	"PING":    ping,
	"SET":     set,
	"GET":     get,
	"HSET":    hset,
	"HGET":    hget,
	"HGETALL": hgetall,
}

// Datastores and mutexs
var (
	SETs = map[string]string{}
	// Mutex used to control concurrent SET requests
	SETsMU = sync.RWMutex{}

	HSETs   = map[string]map[string]string{}
	HSETsMU = sync.RWMutex{}
)

// PING command
// docs: https://redis.io/docs/latest/commands/ping/
func ping(args []Value) Value {
	if len(args) == 0 {
		return Value{typ: "string", str: "PONG"}
	}

	return Value{typ: "string", str: args[0].bulk}
}

// SET command
// docs: https://redis.io/docs/latest/commands/set/
func set(args []Value) Value {
	if len(args) != 2 {
		return Value{
			typ: "error",
			str: fmt.Sprintf("Invalid command, expected 2 arguments, got %d", len(args)),
		}
	}

	key := args[0].bulk
	value := args[1].bulk

	// set value with concurrency control
	SETsMU.Lock()
	defer SETsMU.Unlock()

	SETs[key] = value

	return Value{typ: "string", str: "OK"}
}

// GET command
// docs: https://redis.io/docs/latest/commands/get/
func get(args []Value) Value {
	if len(args) != 1 {
		return Value{
			typ: "error",
			str: fmt.Sprintf("Invalid command, expected 1 argument, got %d", len(args)),
		}
	}

	key := args[0].bulk

	// read value with concurrency control
	SETsMU.RLock()
	defer SETsMU.RUnlock()

	value, ok := SETs[key]

	// key not found, return nil
	if !ok {
		return Value{
			typ: "null",
		}
	}

	return Value{typ: "string", str: value}
}

// HSET command
// docs: https://redis.io/docs/latest/commands/hset/
func hset(args []Value) Value {
	// check if 2n + 1 args passed (key + n pairs of (field, value)), n >= 1
	if len(args) < 3 || (len(args)-1)%2 != 0 {
		return Value{
			typ: "error",
			str: fmt.Sprintf("Invalid command, expected 2n+1 arguments, got %d", len(args)),
		}
	}

	key := args[0].bulk

	// set value with concurrency control
	HSETsMU.Lock()
	defer HSETsMU.Unlock()

	// get map, check if exists, create if it does not exist
	if _, ok := HSETs[key]; !ok {
		HSETs[key] = make(map[string]string)
	}

	// set field, value pairs in map
	for i := 1; i < len(args); i += 2 {
		field := args[i].bulk
		value := args[i+1].bulk
		HSETs[key][field] = value
	}

	return Value{typ: "string", str: "OK"}
}

// HGET command
// docs: https://redis.io/docs/latest/commands/hget/
func hget(args []Value) Value {
	if len(args) != 2 {
		return Value{
			typ: "error",
			str: fmt.Sprintf("Invalid command, expected 2 arguments, got %d",
				len(args)),
		}
	}

	key := args[0].bulk
	field := args[1].bulk

	// get value with concurrency control
	HSETsMU.RLock()
	defer HSETsMU.RUnlock()

	value, ok := HSETs[key][field] // WOW, i had no idea you could do this with nested maps
	if !ok {
		return Value{typ: "null"}
	}

	return Value{typ: "string", str: value}
}

// HGETALL command
// docs: https://redis.io/docs/latest/commands/hgetall/
func hgetall(args []Value) Value {
	if len(args) != 1 {
		return Value{
			typ: "error",
			str: fmt.Sprintf("Invalid command, expected 1 arguments, got %d",
				len(args)),
		}
	}

    key := args[0].bulk

	// get values with concurrency control
	HSETsMU.RLock()
	defer HSETsMU.RUnlock()

    if _, ok := HSETs[key]; !ok {
        // BUG: when input a key that does not exist, the program responds correctly
        // until the first successful HGETALL is used. After HGETALL is used successfully
        // once, the program crashes with a cli output of EOF.
        // Println debugging indicated that this if statement
        // was not even reached in the failure case.
        // Hypothesis: There is something wrong with the Mutexes (maybe using defer)
        // Not sure if this is true because I was not even reaching the RLock() call above
        // Hypothesis: EOF is related to the file reader or writer
        // TODO: setup vim Go debugger to figure out what is happening
        return Value{typ: "array", array: []Value{}} 
    }

    values := []Value{}
    for f, v := range HSETs[key] {
        values = append(values, Value{typ: "bulk", bulk: f})
        values = append(values, Value{typ: "bulk", bulk: v})
    }

    return Value{typ: "array", array: values}
}
