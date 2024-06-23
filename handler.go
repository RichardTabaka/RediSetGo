package main

import "sync"

const (
	ValueTypeArray  = "array"
	ValueTypeString = "string"
	ValueTypeError  = "error"
	ValueTypeBulk   = "bulk"
	ValueTypeNull   = "null"

	ErrorHGetArgs    = "Error: HGET command expects 2 arguments(hash, key)"
	ErrorHGetAllArgs = "Error: HGETALL command expects 1 argument (hash)"
	ErrorHSetArgs    = "Error: HSET command expects 3 agruments(hash, key, value)"
	ErrorGetArgs     = "Error: GET command expects 1 argument(key)"
	ErrorSetArgs     = "Error: SET command expects 2 arguments(key, value)"
)

var Handlers = map[string]func([]Value) Value{
	"HGET":    hGet,
	"HGETALL": hGetAll,
	"HSET":    hSet,
	"GET":     get,
	"PING":    ping,
	"SET":     set,
}

var SETs = map[string]string{}
var SETsMutex = sync.RWMutex{}

var HSETs = map[string]map[string]string{}
var HSETsMutex = sync.RWMutex{}

// The ping command, like an echo with a default of PONG
func ping(args []Value) Value {
	if len(args) == 0 {
		return Value{valueType: ValueTypeString, str: "PONG"}
	}

	return Value{valueType: ValueTypeString, str: args[0].bulk}
}

// HGET returns the value for a key within a specified hash
func hGet(args []Value) Value {
	if len(args) != 2 {
		return Value{
			valueType: ValueTypeError,
			str:       ErrorHGetArgs,
		}
	}

	hashName := args[0].bulk
	key := args[1].bulk

	HSETsMutex.RLock()
	value, ok := HSETs[hashName][key]
	HSETsMutex.RUnlock()

	if !ok {
		return Value{valueType: ValueTypeNull}
	}

	return Value{valueType: ValueTypeBulk, bulk: value}
}

// HGETALL returns all the values from a given hashmap
func hGetAll(args []Value) Value {
	if len(args) != 1 {
		return Value{
			valueType: ValueTypeError,
			str:       ErrorHGetArgs,
		}
	}

	hashName := args[0].bulk

	HSETsMutex.RLock()
	hash, ok := HSETs[hashName]
	HSETsMutex.RUnlock()

	if !ok {
		return Value{valueType: ValueTypeNull}
	}

	// Convert the hashmap to the expected array
	var result []Value
	for _, val := range hash {
		result = append(result, Value{
			valueType: ValueTypeBulk,
			bulk:      val,
		})
	}

	return Value{valueType: ValueTypeArray, array: result}
}

// HSET creates key values within specific hashmaps
func hSet(args []Value) Value {
	// expects hash name, key, value
	if len(args) != 3 {
		return Value{
			valueType: ValueTypeError,
			str:       ErrorHSetArgs,
		}
	}

	hash := args[0].bulk
	key := args[1].bulk
	value := args[2].bulk

	HSETsMutex.Lock()
	// if the specified hash doesn't exist yet, create it
	if _, ok := HSETs[hash]; !ok {
		HSETs[hash] = map[string]string{}
	}
	HSETs[hash][key] = value
	HSETsMutex.Unlock()

	return Value{valueType: ValueTypeString, str: "OK"}
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
