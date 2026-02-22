package main

import (
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"time"
)

type Client struct {
	conn          net.Conn
	writer        *Writer
	writeMu       sync.Mutex
	inMulti       bool
	queue         []Value
	subscriptions map[string]bool
	subMu         sync.Mutex
}

func (c *Client) Send(v Value) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return c.writer.Write(v)
}

func main() {
	fmt.Println("Listening on port :6379")

	// Start background expiry loop
	store.StartExpiryLoop()

	// Load persistence: try AOF first, fall back to RDB
	aof, err := NewAof("database.aof")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer aof.Close()

	aofInfo, _ := os.Stat("database.aof")
	if aofInfo != nil && aofInfo.Size() > 0 {
		fmt.Println("Loading data from AOF...")
		aof.Read(func(value Value) {
			command := strings.ToUpper(value.array[0].bulk)
			args := value.array[1:]
			handler, ok := Handlers[command]
			if !ok {
				return
			}
			handler(args)
		})
		fmt.Println("AOF loaded")
	} else if _, err := os.Stat("dump.rdb"); err == nil {
		fmt.Println("Loading data from RDB...")
		if err := LoadRDB("dump.rdb"); err != nil {
			fmt.Println("RDB load error:", err)
		} else {
			fmt.Println("RDB loaded")
		}
	}

	// Start periodic RDB snapshots (every 5 minutes)
	StartRDBLoop("dump.rdb", 5*time.Minute)

	// Add BGSAVE handler (needs aof reference, defined inline)
	Handlers["BGSAVE"] = func(args []Value) Value {
		go func() {
			if err := SaveRDB("dump.rdb"); err != nil {
				fmt.Println("BGSAVE error:", err)
			} else {
				fmt.Println("BGSAVE completed")
			}
		}()
		return Value{typ: "string", str: "Background saving started"}
	}

	l, err := net.Listen("tcp", ":6379")
	if err != nil {
		fmt.Println(err)
		return
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println(err)
			continue
		}
		go handleConnection(conn, aof)
	}
}

func handleConnection(conn net.Conn, aof *Aof) {
	client := &Client{
		conn:          conn,
		writer:        NewWriter(conn),
		subscriptions: make(map[string]bool),
	}
	defer func() {
		pubsub.UnsubscribeAll(client)
		conn.Close()
	}()

	for {
		resp := NewResp(conn)
		value, err := resp.Read()
		if err != nil {
			return
		}

		if value.typ != "array" {
			continue
		}
		if len(value.array) == 0 {
			continue
		}

		command := strings.ToUpper(value.array[0].bulk)
		args := value.array[1:]

		// --- Transaction handling ---
		if command == "MULTI" {
			client.inMulti = true
			client.queue = nil
			client.Send(Value{typ: "string", str: "OK"})
			continue
		}

		if command == "DISCARD" {
			if !client.inMulti {
				client.Send(Value{typ: "error", str: "ERR DISCARD without MULTI"})
				continue
			}
			client.inMulti = false
			client.queue = nil
			client.Send(Value{typ: "string", str: "OK"})
			continue
		}

		if command == "EXEC" {
			if !client.inMulti {
				client.Send(Value{typ: "error", str: "ERR EXEC without MULTI"})
				continue
			}
			client.inMulti = false

			results := make([]Value, 0, len(client.queue))
			for _, queued := range client.queue {
				cmd := strings.ToUpper(queued.array[0].bulk)
				cmdArgs := queued.array[1:]

				handler, ok := Handlers[cmd]
				if !ok {
					results = append(results, Value{typ: "error", str: "ERR unknown command '" + cmd + "'"})
					continue
				}

				// Log write commands to AOF
				if WriteCommands[cmd] {
					aof.Write(queued)
				}

				results = append(results, handler(cmdArgs))
			}
			client.queue = nil
			client.Send(Value{typ: "array", array: results})
			continue
		}

		// If inside MULTI, queue the command
		if client.inMulti {
			client.queue = append(client.queue, value)
			client.Send(Value{typ: "string", str: "QUEUED"})
			continue
		}

		// --- Pub/Sub handling ---
		if command == "SUBSCRIBE" {
			if len(args) == 0 {
				client.Send(Value{typ: "error", str: "ERR wrong number of arguments for 'subscribe' command"})
				continue
			}
			for _, arg := range args {
				channel := arg.bulk
				count := pubsub.Subscribe(client, channel)
				client.Send(Value{
					typ: "array",
					array: []Value{
						{typ: "bulk", bulk: "subscribe"},
						{typ: "bulk", bulk: channel},
						{typ: "integer", num: count},
					},
				})
			}
			continue
		}

		if command == "UNSUBSCRIBE" {
			if len(args) == 0 {
				// Unsubscribe from all
				client.subMu.Lock()
				channels := make([]string, 0, len(client.subscriptions))
				for ch := range client.subscriptions {
					channels = append(channels, ch)
				}
				client.subMu.Unlock()

				if len(channels) == 0 {
					client.Send(Value{
						typ: "array",
						array: []Value{
							{typ: "bulk", bulk: "unsubscribe"},
							{typ: "null"},
							{typ: "integer", num: 0},
						},
					})
				} else {
					for _, ch := range channels {
						count := pubsub.Unsubscribe(client, ch)
						client.Send(Value{
							typ: "array",
							array: []Value{
								{typ: "bulk", bulk: "unsubscribe"},
								{typ: "bulk", bulk: ch},
								{typ: "integer", num: count},
							},
						})
					}
				}
				continue
			}

			for _, arg := range args {
				channel := arg.bulk
				count := pubsub.Unsubscribe(client, channel)
				client.Send(Value{
					typ: "array",
					array: []Value{
						{typ: "bulk", bulk: "unsubscribe"},
						{typ: "bulk", bulk: channel},
						{typ: "integer", num: count},
					},
				})
			}
			continue
		}

		if command == "PUBLISH" {
			if len(args) != 2 {
				client.Send(Value{typ: "error", str: "ERR wrong number of arguments for 'publish' command"})
				continue
			}
			channel := args[0].bulk
			message := args[1].bulk
			count := pubsub.Publish(channel, message)
			client.Send(Value{typ: "integer", num: count})
			continue
		}

		// --- Regular command handling ---
		handler, ok := Handlers[command]
		if !ok {
			client.Send(Value{typ: "error", str: "ERR unknown command '" + command + "'"})
			continue
		}

		// Log write commands to AOF
		if WriteCommands[command] {
			aof.Write(value)
		}

		result := handler(args)
		client.Send(result)
	}
}
