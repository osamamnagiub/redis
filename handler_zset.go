package main

import (
	"strconv"
	"strings"
)

func zadd(args []Value) Value {
	if len(args) < 3 || len(args[1:])%2 != 0 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'zadd' command"}
	}

	key := args[0].bulk

	store.mu.Lock()
	defer store.mu.Unlock()

	store.checkExpireLocked(key)

	if t, ok := store.keyType[key]; ok && t != "zset" {
		return Value{typ: "error", str: "WRONGTYPE Operation against a key holding the wrong kind of value"}
	}

	if _, ok := store.zsets[key]; !ok {
		store.zsets[key] = &ZSet{members: make(map[string]float64)}
	}

	added := 0
	for i := 1; i < len(args); i += 2 {
		score, err := strconv.ParseFloat(args[i].bulk, 64)
		if err != nil {
			return Value{typ: "error", str: "ERR value is not a valid float"}
		}
		member := args[i+1].bulk

		if _, ok := store.zsets[key].members[member]; !ok {
			added++
		}
		store.zsets[key].members[member] = score
	}
	store.keyType[key] = "zset"

	return Value{typ: "integer", num: added}
}

func zrange(args []Value) Value {
	if len(args) < 3 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'zrange' command"}
	}

	key := args[0].bulk
	start, err := strconv.Atoi(args[1].bulk)
	if err != nil {
		return Value{typ: "error", str: "ERR value is not an integer or out of range"}
	}
	stop, err := strconv.Atoi(args[2].bulk)
	if err != nil {
		return Value{typ: "error", str: "ERR value is not an integer or out of range"}
	}

	withScores := false
	if len(args) > 3 && strings.ToUpper(args[3].bulk) == "WITHSCORES" {
		withScores = true
	}

	store.mu.Lock()
	store.checkExpireLocked(key)

	if t, ok := store.keyType[key]; ok && t != "zset" {
		store.mu.Unlock()
		return Value{typ: "error", str: "WRONGTYPE Operation against a key holding the wrong kind of value"}
	}

	zs, ok := store.zsets[key]
	if !ok {
		store.mu.Unlock()
		return Value{typ: "array", array: []Value{}}
	}

	entries := zs.RankedEntries()
	store.mu.Unlock()

	length := len(entries)

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
		result = append(result, Value{typ: "bulk", bulk: entries[i].Member})
		if withScores {
			result = append(result, Value{typ: "bulk", bulk: strconv.FormatFloat(entries[i].Score, 'f', -1, 64)})
		}
	}

	return Value{typ: "array", array: result}
}

func zscore(args []Value) Value {
	if len(args) != 2 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'zscore' command"}
	}

	key := args[0].bulk
	member := args[1].bulk

	store.mu.Lock()
	store.checkExpireLocked(key)

	if t, ok := store.keyType[key]; ok && t != "zset" {
		store.mu.Unlock()
		return Value{typ: "error", str: "WRONGTYPE Operation against a key holding the wrong kind of value"}
	}

	zs, ok := store.zsets[key]
	if !ok {
		store.mu.Unlock()
		return Value{typ: "null"}
	}

	score, ok := zs.members[member]
	store.mu.Unlock()

	if !ok {
		return Value{typ: "null"}
	}

	return Value{typ: "bulk", bulk: strconv.FormatFloat(score, 'f', -1, 64)}
}

func zrank(args []Value) Value {
	if len(args) != 2 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'zrank' command"}
	}

	key := args[0].bulk
	member := args[1].bulk

	store.mu.Lock()
	store.checkExpireLocked(key)

	if t, ok := store.keyType[key]; ok && t != "zset" {
		store.mu.Unlock()
		return Value{typ: "error", str: "WRONGTYPE Operation against a key holding the wrong kind of value"}
	}

	zs, ok := store.zsets[key]
	if !ok {
		store.mu.Unlock()
		return Value{typ: "null"}
	}

	entries := zs.RankedEntries()
	store.mu.Unlock()

	for i, e := range entries {
		if e.Member == member {
			return Value{typ: "integer", num: i}
		}
	}

	return Value{typ: "null"}
}
