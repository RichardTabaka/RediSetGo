package main

import "sync"

const (
	ValueTypeString = "string"
	ValueTypeError  = "error"
	ValueTypeBulk   = "bulk"
	ValueTypeNull   = "null"

	ErrorGetArgs = "Error: GET command expects 1 argument(key)"
	ErrorSetArgs = "Error: SET command expects 2 arguments(key, value)"
)

var Handlers = map[string]func([]Value) Value{
	"GET":  get,
	"PING": ping,
	"SET":  set,
}

var SETs = map[string]string{}
var SETsMutex = sync.RWMutex{}

// The ping command, like an echo with a default of PONG
func ping(args []Value) Value {
	if len(args) == 0 {
		return Value{valueType: ValueTypeString, str: "PONG"}
	}

	return Value{valueType: ValueTypeString, str: args[0].bulk}
}

// GET returns the value from SETs for a given key
func get(args []Value) Value {
	if len(args) != 1 {
		return Value{
			valueType: ValueTypeError,
			str:       ErrorGetArgs,
		}
	}

	key := args[0].bulk

	SETsMutex.RLock()
	value, ok := SETs[key]
	SETsMutex.RUnlock()

	if !ok {
		return Value{valueType: ValueTypeNull}
	}

	return Value{valueType: ValueTypeBulk, bulk: value}
}

// SET creates key value pair to store data
func set(args []Value) Value {
	if len(args) != 2 {
		return Value{
			valueType: ValueTypeError,
			str:       ErrorSetArgs,
		}
	}

	key := args[0].bulk
	value := args[1].bulk

	SETsMutex.Lock()
	SETs[key] = value
	SETsMutex.Unlock()

	return Value{valueType: ValueTypeString, str: "OK"}
}
