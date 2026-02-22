package main

import "sync"

type PubSub struct {
	mu          sync.RWMutex
	subscribers map[string]map[*Client]struct{} // channel -> set of clients
}

var pubsub = &PubSub{
	subscribers: make(map[string]map[*Client]struct{}),
}

func (ps *PubSub) Subscribe(client *Client, channel string) int {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	if _, ok := ps.subscribers[channel]; !ok {
		ps.subscribers[channel] = make(map[*Client]struct{})
	}
	ps.subscribers[channel][client] = struct{}{}

	client.subMu.Lock()
	client.subscriptions[channel] = true
	count := len(client.subscriptions)
	client.subMu.Unlock()

	return count
}

func (ps *PubSub) Unsubscribe(client *Client, channel string) int {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	if subs, ok := ps.subscribers[channel]; ok {
		delete(subs, client)
		if len(subs) == 0 {
			delete(ps.subscribers, channel)
		}
	}

	client.subMu.Lock()
	delete(client.subscriptions, channel)
	count := len(client.subscriptions)
	client.subMu.Unlock()

	return count
}

func (ps *PubSub) UnsubscribeAll(client *Client) {
	client.subMu.Lock()
	channels := make([]string, 0, len(client.subscriptions))
	for ch := range client.subscriptions {
		channels = append(channels, ch)
	}
	client.subMu.Unlock()

	for _, ch := range channels {
		ps.Unsubscribe(client, ch)
	}
}

func (ps *PubSub) Publish(channel, message string) int {
	ps.mu.RLock()
	defer ps.mu.RUnlock()

	subs, ok := ps.subscribers[channel]
	if !ok {
		return 0
	}

	msg := Value{
		typ: "array",
		array: []Value{
			{typ: "bulk", bulk: "message"},
			{typ: "bulk", bulk: channel},
			{typ: "bulk", bulk: message},
		},
	}

	count := 0
	for client := range subs {
		if err := client.Send(msg); err == nil {
			count++
		}
	}

	return count
}
