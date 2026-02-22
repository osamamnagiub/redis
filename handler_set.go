package main

func sadd(args []Value) Value {
	if len(args) < 2 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'sadd' command"}
	}

	key := args[0].bulk

	store.mu.Lock()
	defer store.mu.Unlock()

	store.checkExpireLocked(key)

	if t, ok := store.keyType[key]; ok && t != "set" {
		return Value{typ: "error", str: "WRONGTYPE Operation against a key holding the wrong kind of value"}
	}

	if _, ok := store.sets[key]; !ok {
		store.sets[key] = make(map[string]struct{})
	}

	added := 0
	for _, arg := range args[1:] {
		if _, ok := store.sets[key][arg.bulk]; !ok {
			store.sets[key][arg.bulk] = struct{}{}
			added++
		}
	}
	store.keyType[key] = "set"

	return Value{typ: "integer", num: added}
}

func srem(args []Value) Value {
	if len(args) < 2 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'srem' command"}
	}

	key := args[0].bulk

	store.mu.Lock()
	defer store.mu.Unlock()

	store.checkExpireLocked(key)

	if t, ok := store.keyType[key]; ok && t != "set" {
		return Value{typ: "error", str: "WRONGTYPE Operation against a key holding the wrong kind of value"}
	}

	s, ok := store.sets[key]
	if !ok {
		return Value{typ: "integer", num: 0}
	}

	removed := 0
	for _, arg := range args[1:] {
		if _, ok := s[arg.bulk]; ok {
			delete(s, arg.bulk)
			removed++
		}
	}

	if len(s) == 0 {
		store.deleteKeyLocked(key)
	}

	return Value{typ: "integer", num: removed}
}

func smembers(args []Value) Value {
	if len(args) != 1 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'smembers' command"}
	}

	key := args[0].bulk

	store.mu.Lock()
	store.checkExpireLocked(key)

	if t, ok := store.keyType[key]; ok && t != "set" {
		store.mu.Unlock()
		return Value{typ: "error", str: "WRONGTYPE Operation against a key holding the wrong kind of value"}
	}

	s, ok := store.sets[key]
	store.mu.Unlock()

	if !ok {
		return Value{typ: "array", array: []Value{}}
	}

	var result []Value
	for member := range s {
		result = append(result, Value{typ: "bulk", bulk: member})
	}

	return Value{typ: "array", array: result}
}

func sismember(args []Value) Value {
	if len(args) != 2 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'sismember' command"}
	}

	key := args[0].bulk
	member := args[1].bulk

	store.mu.Lock()
	store.checkExpireLocked(key)

	if t, ok := store.keyType[key]; ok && t != "set" {
		store.mu.Unlock()
		return Value{typ: "error", str: "WRONGTYPE Operation against a key holding the wrong kind of value"}
	}

	s, ok := store.sets[key]
	store.mu.Unlock()

	if !ok {
		return Value{typ: "integer", num: 0}
	}

	if _, ok := s[member]; ok {
		return Value{typ: "integer", num: 1}
	}

	return Value{typ: "integer", num: 0}
}
