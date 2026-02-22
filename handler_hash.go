package main

func hset(args []Value) Value {
	if len(args) != 3 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'hset' command"}
	}

	key := args[0].bulk
	field := args[1].bulk
	value := args[2].bulk

	store.mu.Lock()
	defer store.mu.Unlock()

	store.checkExpireLocked(key)

	if t, ok := store.keyType[key]; ok && t != "hash" {
		return Value{typ: "error", str: "WRONGTYPE Operation against a key holding the wrong kind of value"}
	}

	if _, ok := store.hashes[key]; !ok {
		store.hashes[key] = map[string]string{}
	}

	_, existed := store.hashes[key][field]
	store.hashes[key][field] = value
	store.keyType[key] = "hash"

	added := 0
	if !existed {
		added = 1
	}
	return Value{typ: "integer", num: added}
}

func hget(args []Value) Value {
	if len(args) != 2 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'hget' command"}
	}

	key := args[0].bulk
	field := args[1].bulk

	store.mu.Lock()
	store.checkExpireLocked(key)

	if t, ok := store.keyType[key]; ok && t != "hash" {
		store.mu.Unlock()
		return Value{typ: "error", str: "WRONGTYPE Operation against a key holding the wrong kind of value"}
	}

	value, ok := store.hashes[key][field]
	store.mu.Unlock()

	if !ok {
		return Value{typ: "null"}
	}

	return Value{typ: "bulk", bulk: value}
}

func hgetall(args []Value) Value {
	if len(args) != 1 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'hgetall' command"}
	}

	key := args[0].bulk

	store.mu.Lock()
	store.checkExpireLocked(key)

	if t, ok := store.keyType[key]; ok && t != "hash" {
		store.mu.Unlock()
		return Value{typ: "error", str: "WRONGTYPE Operation against a key holding the wrong kind of value"}
	}

	hash, ok := store.hashes[key]
	store.mu.Unlock()

	if !ok {
		return Value{typ: "array", array: []Value{}}
	}

	var result []Value
	for k, v := range hash {
		result = append(result, Value{typ: "bulk", bulk: k}, Value{typ: "bulk", bulk: v})
	}

	return Value{typ: "array", array: result}
}

func hdel(args []Value) Value {
	if len(args) < 2 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'hdel' command"}
	}

	key := args[0].bulk

	store.mu.Lock()
	defer store.mu.Unlock()

	store.checkExpireLocked(key)

	if t, ok := store.keyType[key]; ok && t != "hash" {
		return Value{typ: "error", str: "WRONGTYPE Operation against a key holding the wrong kind of value"}
	}

	hash, ok := store.hashes[key]
	if !ok {
		return Value{typ: "integer", num: 0}
	}

	count := 0
	for _, arg := range args[1:] {
		if _, ok := hash[arg.bulk]; ok {
			delete(hash, arg.bulk)
			count++
		}
	}

	if len(hash) == 0 {
		store.deleteKeyLocked(key)
	}

	return Value{typ: "integer", num: count}
}
