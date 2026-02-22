package main

import (
	"fmt"
	"path"
	"strconv"
	"strings"
	"time"
)

var Handlers = map[string]func([]Value) Value{
	"PING":    ping,
	"SET":     set,
	"GET":     get,
	"DEL":     del,
	"EXISTS":  exists,
	"TYPE":    keytype,
	"KEYS":    keys,
	"INCR":    incr,
	"DECR":    decr,
	"EXPIRE":  expire,
	"TTL":     ttl,
	"HSET":    hset,
	"HGET":    hget,
	"HGETALL": hgetall,
	"HDEL":    hdel,
	"LPUSH":   lpush,
	"RPUSH":   rpush,
	"LPOP":    lpop,
	"RPOP":    rpop,
	"LRANGE":  lrange,
	"SADD":    sadd,
	"SREM":    srem,
	"SMEMBERS": smembers,
	"SISMEMBER": sismember,
	"ZADD":    zadd,
	"ZRANGE":  zrange,
	"ZSCORE":  zscore,
	"ZRANK":   zrank,
	"DBSIZE":  dbsize,
	"FLUSHDB": flushdb,
	"INFO":    info,
	"SELECT":  selectdb,
	"COMMAND": command,
}

// WriteCommands is the set of commands that modify data and should be logged to AOF.
var WriteCommands = map[string]bool{
	"SET": true, "DEL": true, "INCR": true, "DECR": true,
	"EXPIRE": true,
	"HSET": true, "HDEL": true,
	"LPUSH": true, "RPUSH": true, "LPOP": true, "RPOP": true,
	"SADD": true, "SREM": true,
	"ZADD": true,
	"FLUSHDB": true,
}

func ping(args []Value) Value {
	if len(args) == 0 {
		return Value{typ: "string", str: "PONG"}
	}
	return Value{typ: "string", str: args[0].bulk}
}

func set(args []Value) Value {
	if len(args) < 2 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'set' command"}
	}

	key := args[0].bulk
	value := args[1].bulk

	store.mu.Lock()
	store.deleteKeyLocked(key)
	store.strs[key] = value
	store.keyType[key] = "string"
	store.mu.Unlock()

	return Value{typ: "string", str: "OK"}
}

func get(args []Value) Value {
	if len(args) != 1 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'get' command"}
	}

	key := args[0].bulk

	store.mu.Lock()
	store.checkExpireLocked(key)

	if t, ok := store.keyType[key]; ok && t != "string" {
		store.mu.Unlock()
		return Value{typ: "error", str: "WRONGTYPE Operation against a key holding the wrong kind of value"}
	}

	value, ok := store.strs[key]
	store.mu.Unlock()

	if !ok {
		return Value{typ: "null"}
	}

	return Value{typ: "bulk", bulk: value}
}

func del(args []Value) Value {
	if len(args) == 0 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'del' command"}
	}

	store.mu.Lock()
	count := 0
	for _, arg := range args {
		if _, ok := store.keyType[arg.bulk]; ok {
			store.deleteKeyLocked(arg.bulk)
			count++
		}
	}
	store.mu.Unlock()

	return Value{typ: "integer", num: count}
}

func exists(args []Value) Value {
	if len(args) == 0 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'exists' command"}
	}

	store.mu.Lock()
	count := 0
	for _, arg := range args {
		store.checkExpireLocked(arg.bulk)
		if _, ok := store.keyType[arg.bulk]; ok {
			count++
		}
	}
	store.mu.Unlock()

	return Value{typ: "integer", num: count}
}

func keytype(args []Value) Value {
	if len(args) != 1 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'type' command"}
	}

	key := args[0].bulk

	store.mu.Lock()
	store.checkExpireLocked(key)
	t, ok := store.keyType[key]
	store.mu.Unlock()

	if !ok {
		return Value{typ: "string", str: "none"}
	}

	return Value{typ: "string", str: t}
}

func keys(args []Value) Value {
	if len(args) != 1 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'keys' command"}
	}

	pattern := args[0].bulk
	var result []Value

	store.mu.Lock()
	for key := range store.keyType {
		store.checkExpireLocked(key)
		if _, ok := store.keyType[key]; !ok {
			continue
		}
		matched, err := path.Match(pattern, key)
		if err != nil {
			store.mu.Unlock()
			return Value{typ: "error", str: "ERR invalid pattern"}
		}
		if matched {
			result = append(result, Value{typ: "bulk", bulk: key})
		}
	}
	store.mu.Unlock()

	return Value{typ: "array", array: result}
}

func incr(args []Value) Value {
	if len(args) != 1 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'incr' command"}
	}

	key := args[0].bulk

	store.mu.Lock()
	defer store.mu.Unlock()

	store.checkExpireLocked(key)

	if t, ok := store.keyType[key]; ok && t != "string" {
		return Value{typ: "error", str: "WRONGTYPE Operation against a key holding the wrong kind of value"}
	}

	val := "0"
	if v, ok := store.strs[key]; ok {
		val = v
	}

	num, err := strconv.Atoi(val)
	if err != nil {
		return Value{typ: "error", str: "ERR value is not an integer or out of range"}
	}

	num++
	store.strs[key] = strconv.Itoa(num)
	store.keyType[key] = "string"

	return Value{typ: "integer", num: num}
}

func decr(args []Value) Value {
	if len(args) != 1 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'decr' command"}
	}

	key := args[0].bulk

	store.mu.Lock()
	defer store.mu.Unlock()

	store.checkExpireLocked(key)

	if t, ok := store.keyType[key]; ok && t != "string" {
		return Value{typ: "error", str: "WRONGTYPE Operation against a key holding the wrong kind of value"}
	}

	val := "0"
	if v, ok := store.strs[key]; ok {
		val = v
	}

	num, err := strconv.Atoi(val)
	if err != nil {
		return Value{typ: "error", str: "ERR value is not an integer or out of range"}
	}

	num--
	store.strs[key] = strconv.Itoa(num)
	store.keyType[key] = "string"

	return Value{typ: "integer", num: num}
}

func expire(args []Value) Value {
	if len(args) != 2 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'expire' command"}
	}

	key := args[0].bulk
	seconds, err := strconv.Atoi(args[1].bulk)
	if err != nil {
		return Value{typ: "error", str: "ERR value is not an integer or out of range"}
	}

	store.mu.Lock()
	defer store.mu.Unlock()

	store.checkExpireLocked(key)

	if _, ok := store.keyType[key]; !ok {
		return Value{typ: "integer", num: 0}
	}

	store.expires[key] = time.Now().Add(time.Duration(seconds) * time.Second)
	return Value{typ: "integer", num: 1}
}

func ttl(args []Value) Value {
	if len(args) != 1 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'ttl' command"}
	}

	key := args[0].bulk

	store.mu.Lock()
	defer store.mu.Unlock()

	store.checkExpireLocked(key)

	if _, ok := store.keyType[key]; !ok {
		return Value{typ: "integer", num: -2} // key does not exist
	}

	exp, ok := store.expires[key]
	if !ok {
		return Value{typ: "integer", num: -1} // key exists but no expiry
	}

	remaining := int(time.Until(exp).Seconds())
	if remaining < 0 {
		return Value{typ: "integer", num: -2}
	}

	return Value{typ: "integer", num: remaining}
}

func dbsize(args []Value) Value {
	store.mu.RLock()
	count := store.KeyCount()
	store.mu.RUnlock()
	return Value{typ: "integer", num: count}
}

func flushdb(args []Value) Value {
	store.mu.Lock()
	store.Flush()
	store.mu.Unlock()
	return Value{typ: "string", str: "OK"}
}

func selectdb(args []Value) Value {
	return Value{typ: "string", str: "OK"}
}

func command(args []Value) Value {
	return Value{typ: "array", array: []Value{}}
}

func info(args []Value) Value {
	store.mu.RLock()
	keyCount := store.KeyCount()
	expireCount := store.ExpireCount()
	store.mu.RUnlock()

	sections := []string{
		"# Server",
		"redis_version:0.2.0-go-clone",
		"tcp_port:6379",
		"",
		"# Keyspace",
		fmt.Sprintf("db0:keys=%d,expires=%d", keyCount, expireCount),
	}

	return Value{typ: "bulk", bulk: strings.Join(sections, "\r\n")}
}
