package main

import "strconv"

func lpush(args []Value) Value {
	if len(args) < 2 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'lpush' command"}
	}

	key := args[0].bulk

	store.mu.Lock()
	defer store.mu.Unlock()

	store.checkExpireLocked(key)

	if t, ok := store.keyType[key]; ok && t != "list" {
		return Value{typ: "error", str: "WRONGTYPE Operation against a key holding the wrong kind of value"}
	}

	for _, arg := range args[1:] {
		store.lists[key] = append([]string{arg.bulk}, store.lists[key]...)
	}
	store.keyType[key] = "list"

	return Value{typ: "integer", num: len(store.lists[key])}
}

func rpush(args []Value) Value {
	if len(args) < 2 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'rpush' command"}
	}

	key := args[0].bulk

	store.mu.Lock()
	defer store.mu.Unlock()

	store.checkExpireLocked(key)

	if t, ok := store.keyType[key]; ok && t != "list" {
		return Value{typ: "error", str: "WRONGTYPE Operation against a key holding the wrong kind of value"}
	}

	for _, arg := range args[1:] {
		store.lists[key] = append(store.lists[key], arg.bulk)
	}
	store.keyType[key] = "list"

	return Value{typ: "integer", num: len(store.lists[key])}
}

func lpop(args []Value) Value {
	if len(args) != 1 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'lpop' command"}
	}

	key := args[0].bulk

	store.mu.Lock()
	defer store.mu.Unlock()

	store.checkExpireLocked(key)

	if t, ok := store.keyType[key]; ok && t != "list" {
		return Value{typ: "error", str: "WRONGTYPE Operation against a key holding the wrong kind of value"}
	}

	list, ok := store.lists[key]
	if !ok || len(list) == 0 {
		return Value{typ: "null"}
	}

	val := list[0]
	store.lists[key] = list[1:]

	if len(store.lists[key]) == 0 {
		store.deleteKeyLocked(key)
	}

	return Value{typ: "bulk", bulk: val}
}

func rpop(args []Value) Value {
	if len(args) != 1 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'rpop' command"}
	}

	key := args[0].bulk

	store.mu.Lock()
	defer store.mu.Unlock()

	store.checkExpireLocked(key)

	if t, ok := store.keyType[key]; ok && t != "list" {
		return Value{typ: "error", str: "WRONGTYPE Operation against a key holding the wrong kind of value"}
	}

	list, ok := store.lists[key]
	if !ok || len(list) == 0 {
		return Value{typ: "null"}
	}

	val := list[len(list)-1]
	store.lists[key] = list[:len(list)-1]

	if len(store.lists[key]) == 0 {
		store.deleteKeyLocked(key)
	}

	return Value{typ: "bulk", bulk: val}
}

func lrange(args []Value) Value {
	if len(args) != 3 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'lrange' command"}
	}

	key := args[0].bulk
	startStr := args[1].bulk
	stopStr := args[2].bulk

	start, err := strconv.Atoi(startStr)
	if err != nil {
		return Value{typ: "error", str: "ERR value is not an integer or out of range"}
	}
	stop, err := strconv.Atoi(stopStr)
	if err != nil {
		return Value{typ: "error", str: "ERR value is not an integer or out of range"}
	}

	store.mu.Lock()
	store.checkExpireLocked(key)

	if t, ok := store.keyType[key]; ok && t != "list" {
		store.mu.Unlock()
		return Value{typ: "error", str: "WRONGTYPE Operation against a key holding the wrong kind of value"}
	}

	list, ok := store.lists[key]
	store.mu.Unlock()

	if !ok {
		return Value{typ: "array", array: []Value{}}
	}

	length := len(list)

	// Handle negative indices
	if start < 0 {
		start = length + start
	}
	if stop < 0 {
		stop = length + stop
	}
	if start < 0 {
		start = 0
	}
	if stop >= length {
		stop = length - 1
	}
	if start > stop {
		return Value{typ: "array", array: []Value{}}
	}

	var result []Value
	for i := start; i <= stop; i++ {
		result = append(result, Value{typ: "bulk", bulk: list[i]})
	}

	return Value{typ: "array", array: result}
}
