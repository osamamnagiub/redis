# Redis Clone in Go

A Redis-compatible in-memory data store built from scratch in Go. Implements the RESP (Redis Serialization Protocol) and supports persistent storage via AOF and RDB snapshots.

## Features

### Data Structures
- **Strings** - SET, GET, DEL, INCR, DECR
- **Hashes** - HSET, HGET, HGETALL, HDEL
- **Lists** - LPUSH, RPUSH, LPOP, RPOP, LRANGE
- **Sets** - SADD, SREM, SMEMBERS, SISMEMBER
- **Sorted Sets** - ZADD, ZRANGE (with WITHSCORES), ZSCORE, ZRANK

### Key Management
- EXPIRE, TTL - key expiration with background cleanup
- EXISTS, TYPE, KEYS - key inspection with glob pattern matching
- DEL - delete one or more keys
- DBSIZE, FLUSHDB - database-level operations

### Persistence
- **AOF (Append-Only File)** - write-ahead log that records every write command in RESP format, replayed on startup to restore state, fsync every 1 second
- **RDB Snapshots** - periodic point-in-time JSON snapshots of the full dataset, auto-save every 5 minutes, manual trigger via BGSAVE

### Pub/Sub
- SUBSCRIBE, UNSUBSCRIBE, PUBLISH - channel-based message broadcasting

### Transactions
- MULTI, EXEC, DISCARD - command queuing with atomic execution

### Server
- Multi-client support via goroutine-per-connection
- Full RESP protocol parser and writer
- INFO, SELECT, COMMAND for client compatibility

## Getting Started

### Prerequisites
- Go 1.26+

### Build and Run

```bash
go build -o redis-server .
./redis-server
```

The server listens on port `6379` (same as Redis).

### Connect with redis-cli

```bash
redis-cli

# Strings
SET name "osama"
GET name
INCR counter

# Hashes
HSET user name "osama"
HGET user name

# Lists
RPUSH mylist a b c
LRANGE mylist 0 -1

# Sets
SADD myset apple banana cherry
SMEMBERS myset

# Sorted Sets
ZADD leaderboard 100 alice 200 bob
ZRANGE leaderboard 0 -1 WITHSCORES

# Expiration
EXPIRE name 60
TTL name

# Transactions
MULTI
SET x 1
SET y 2
EXEC

# Pub/Sub (in separate terminals)
SUBSCRIBE mychannel       # terminal 1
PUBLISH mychannel "hello" # terminal 2

# Persistence
BGSAVE
```

## Architecture

```
resp.go            RESP protocol parser/writer
store.go           Centralized data store with expiration
handler.go         String/key commands + command registry
handler_hash.go    Hash commands
handler_list.go    List commands
handler_set.go     Set commands
handler_zset.go    Sorted set commands
aof.go             Append-Only File persistence
rdb.go             RDB snapshot persistence
pubsub.go          Pub/Sub subscription manager
main.go            TCP server, client handling, transactions
```

### Persistence Strategy

On startup, the server restores data in this order:
1. If `database.aof` exists and is non-empty, replay the AOF
2. Otherwise, if `dump.rdb` exists, load the RDB snapshot

Write commands are appended to the AOF in real-time. RDB snapshots are saved every 5 minutes as a safety net.
