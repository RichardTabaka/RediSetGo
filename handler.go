package main

import (
	"fmt"
	"strings"
	"sync"
)

const (
	ValueTypeArray   = "array"
	ValueTypeBulk    = "bulk"
	ValueTypeError   = "error"
	ValueTypeInteger = "integer"
	ValueTypeNull    = "null"
	ValueTypeString  = "string"

	ErrorDelArgs     = "Error: DEL command expects at least 1 argument(key)"
	ErrorGetArgs     = "Error: GET command expects 1 argument(key)"
	ErrorHDelArgs    = "Error: HDEL command expects at least 2 arguments(hash, key)"
	ErrorHGetArgs    = "Error: HGET command expects 2 arguments(hash, key)"
	ErrorHGetAllArgs = "Error: HGETALL command expects 1 argument (hash)"
	ErrorHSetArgs    = "Error: HSET command expects 3 agruments(hash, key, value)"
	ErrorKeyArgs     = "Error: KEYS command expects 1 argument(pattern)"
	ErrorSetArgs     = "Error: SET command expects 2 arguments(key, value)"
)

var Handlers = map[string]func([]Value) Value{
	"DEL":     del,
	"HDEL":    hDel,
	"HGET":    hGet,
	"HGETALL": hGetAll,
	"HSET":    hSet,
	"GET":     get,
	"KEYS":    keys,
	"PING":    ping,
	"SET":     set,
}

var SETs = map[string]string{}
var SETsMutex = sync.RWMutex{}

var HSETs = map[string]map[string]string{}
var HSETsMutex = sync.RWMutex{}

func GetCurrentState() map[string]Value {
	currentState := make(map[string]Value)

	// Get current SETs
	SETsMutex.RLock()
	for key, value := range SETs {
		currentState[key] = Value{
			valueType: ValueTypeString,
			str:       value,
		}
	}
	SETsMutex.RUnlock()

	// Get current HSETs
	HSETsMutex.RLock()
	for hashName, hash := range HSETs {
		for key, value := range hash {
			fullKey := fmt.Sprintf("%s:%s", hashName, key)
			currentState[fullKey] = Value{
				valueType: ValueTypeBulk,
				bulk:      value,
			}
		}
	}
	HSETsMutex.RUnlock()

	return currentState
}

// del will remove specified key(s) and corresponding value
func del(args []Value) Value {
	if len(args) == 0 {
		return Value{
			valueType: ValueTypeError,
			str:       ErrorDelArgs,
		}
	}

	SETsMutex.Lock()
	HSETsMutex.Lock()
	defer SETsMutex.Unlock()
	defer HSETsMutex.Unlock()

	deletedCount := 0
	for _, arg := range args {
		key := arg.bulk

		if _, ok := SETs[key]; ok {
			delete(SETs, key)
			deletedCount++
		} else if _, ok := HSETs[key]; ok {
			delete(HSETs, key)
			deletedCount++
		}
	}

	return Value{
		valueType: ValueTypeInteger,
		num:       deletedCount,
	}
}

// The ping command, like an echo with a default of PONG
func ping(args []Value) Value {
	if len(args) == 0 {
		return Value{
			valueType: ValueTypeString,
			str:       "PONG",
		}
	}

	return Value{
		valueType: ValueTypeString,
		str:       args[0].bulk,
	}
}

// HDEL deletes an item from a specified hashmap
func hDel(args []Value) Value {
	if len(args) < 2 {
		return Value{
			valueType: ValueTypeError,
			str:       ErrorHDelArgs,
		}
	}

	hashName := args[0].bulk
	HSETsMutex.Lock()
	defer HSETsMutex.Unlock()

	deletedCount := 0
	if _, ok := HSETs[hashName]; !ok {
		return Value{
			valueType: ValueTypeInteger,
			num:       deletedCount,
		}
	}

	for i := 1; i < len(args); i++ {
		key := args[i].bulk
		if _, ok := HSETs[hashName][key]; ok {
			delete(HSETs[hashName], key)
			deletedCount++
		}
	}

	return Value{
		valueType: ValueTypeInteger,
		num:       deletedCount,
	}
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

	return Value{
		valueType: ValueTypeBulk,
		bulk:      value,
	}
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

	return Value{
		valueType: ValueTypeArray,
		array:     result,
	}
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

	return Value{
		valueType: ValueTypeString,
		str:       "OK",
	}
}

// keys returns all keys matching a specified pattern
func keys(args []Value) Value {
	if len(args) != 1 {
		return Value{
			valueType: ValueTypeError,
			str:       ErrorKeyArgs,
		}
	}

	pattern := args[0].bulk

	var matchingKeys []string

	// Check SETs for any matches
	SETsMutex.RLock()
	for key := range SETs {
		if matchPattern(pattern, key) {
			matchingKeys = append(matchingKeys, key)
		}
	}
	SETsMutex.RUnlock()

	// Also check HSETs for any key matches
	HSETsMutex.RLock()
	for hashName := range HSETs {
		for key := range HSETs[hashName] {
			fullKey := fmt.Sprintf("%s:%s", hashName, key)
			if matchPattern(pattern, fullKey) {
				matchingKeys = append(matchingKeys, fullKey)
			}
		}
	}
	HSETsMutex.RUnlock()

	// build result set
	var result []Value
	for _, key := range matchingKeys {
		result = append(result, Value{
			valueType: ValueTypeBulk,
			bulk:      key,
		})
	}

	return Value{
		valueType: ValueTypeArray,
		array:     result,
	}
}

// helper function for pattern matching
func matchPattern(pattern, key string) bool {
	return pattern == "*" || strings.Contains(key, pattern)
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

	return Value{
		valueType: ValueTypeBulk,
		bulk:      value,
	}
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

	return Value{
		valueType: ValueTypeString,
		str:       "OK",
	}
}
