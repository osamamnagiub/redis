package main

import (
	"sort"
	"sync"
	"time"
)

var store = &Store{
	strs:    make(map[string]string),
	hashes:  make(map[string]map[string]string),
	lists:   make(map[string][]string),
	sets:    make(map[string]map[string]struct{}),
	zsets:   make(map[string]*ZSet),
	keyType: make(map[string]string),
	expires: make(map[string]time.Time),
}

type Store struct {
	mu      sync.RWMutex
	strs    map[string]string
	hashes  map[string]map[string]string
	lists   map[string][]string
	sets    map[string]map[string]struct{}
	zsets   map[string]*ZSet
	keyType map[string]string
	expires map[string]time.Time
}

type ZSet struct {
	members map[string]float64
}

type ZSetEntry struct {
	Member string
	Score  float64
}

func (z *ZSet) RankedEntries() []ZSetEntry {
	entries := make([]ZSetEntry, 0, len(z.members))
	for m, s := range z.members {
		entries = append(entries, ZSetEntry{m, s})
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].Score == entries[j].Score {
			return entries[i].Member < entries[j].Member
		}
		return entries[i].Score < entries[j].Score
	})
	return entries
}

// deleteKeyLocked removes a key from all data structures. Caller must hold write lock.
func (s *Store) deleteKeyLocked(key string) {
	typ := s.keyType[key]
	switch typ {
	case "string":
		delete(s.strs, key)
	case "hash":
		delete(s.hashes, key)
	case "list":
		delete(s.lists, key)
	case "set":
		delete(s.sets, key)
	case "zset":
		delete(s.zsets, key)
	}
	delete(s.keyType, key)
	delete(s.expires, key)
}

// checkExpireLocked checks if a key is expired and deletes it if so. Caller must hold write lock.
func (s *Store) checkExpireLocked(key string) bool {
	exp, ok := s.expires[key]
	if !ok {
		return false
	}
	if time.Now().After(exp) {
		s.deleteKeyLocked(key)
		return true
	}
	return false
}

// StartExpiryLoop runs a background goroutine that periodically cleans up expired keys.
func (s *Store) StartExpiryLoop() {
	go func() {
		for {
			time.Sleep(100 * time.Millisecond)
			s.mu.Lock()
			now := time.Now()
			for key, exp := range s.expires {
				if now.After(exp) {
					s.deleteKeyLocked(key)
				}
			}
			s.mu.Unlock()
		}
	}()
}

// Flush clears all data from the store.
func (s *Store) Flush() {
	s.strs = make(map[string]string)
	s.hashes = make(map[string]map[string]string)
	s.lists = make(map[string][]string)
	s.sets = make(map[string]map[string]struct{})
	s.zsets = make(map[string]*ZSet)
	s.keyType = make(map[string]string)
	s.expires = make(map[string]time.Time)
}

// KeyCount returns the total number of keys.
func (s *Store) KeyCount() int {
	return len(s.keyType)
}

// ExpireCount returns the number of keys with an expiry set.
func (s *Store) ExpireCount() int {
	return len(s.expires)
}
