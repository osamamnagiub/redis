package main

import (
	"encoding/json"
	"fmt"
	"os"
	"time"
)

type RDBSnapshot struct {
	Strs    map[string]string              `json:"strings"`
	Hashes  map[string]map[string]string   `json:"hashes"`
	Lists   map[string][]string            `json:"lists"`
	Sets    map[string][]string            `json:"sets"`
	ZSets   map[string]map[string]float64  `json:"zsets"`
	Expires map[string]int64               `json:"expires"`
}

func SaveRDB(path string) error {
	store.mu.RLock()

	snap := RDBSnapshot{
		Strs:    make(map[string]string),
		Hashes:  make(map[string]map[string]string),
		Lists:   make(map[string][]string),
		Sets:    make(map[string][]string),
		ZSets:   make(map[string]map[string]float64),
		Expires: make(map[string]int64),
	}

	for k, v := range store.strs {
		snap.Strs[k] = v
	}
	for k, v := range store.hashes {
		snap.Hashes[k] = make(map[string]string)
		for fk, fv := range v {
			snap.Hashes[k][fk] = fv
		}
	}
	for k, v := range store.lists {
		snap.Lists[k] = make([]string, len(v))
		copy(snap.Lists[k], v)
	}
	for k, v := range store.sets {
		members := make([]string, 0, len(v))
		for m := range v {
			members = append(members, m)
		}
		snap.Sets[k] = members
	}
	for k, v := range store.zsets {
		snap.ZSets[k] = make(map[string]float64)
		for m, s := range v.members {
			snap.ZSets[k][m] = s
		}
	}
	for k, v := range store.expires {
		snap.Expires[k] = v.Unix()
	}

	store.mu.RUnlock()

	data, err := json.Marshal(snap)
	if err != nil {
		return fmt.Errorf("failed to marshal RDB: %w", err)
	}

	tmpPath := path + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0666); err != nil {
		return fmt.Errorf("failed to write RDB: %w", err)
	}

	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("failed to rename RDB: %w", err)
	}

	return nil
}

func LoadRDB(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	var snap RDBSnapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return fmt.Errorf("failed to unmarshal RDB: %w", err)
	}

	store.mu.Lock()
	defer store.mu.Unlock()

	store.Flush()

	for k, v := range snap.Strs {
		store.strs[k] = v
		store.keyType[k] = "string"
	}
	for k, v := range snap.Hashes {
		store.hashes[k] = v
		store.keyType[k] = "hash"
	}
	for k, v := range snap.Lists {
		store.lists[k] = v
		store.keyType[k] = "list"
	}
	for k, members := range snap.Sets {
		store.sets[k] = make(map[string]struct{})
		for _, m := range members {
			store.sets[k][m] = struct{}{}
		}
		store.keyType[k] = "set"
	}
	for k, members := range snap.ZSets {
		store.zsets[k] = &ZSet{members: members}
		store.keyType[k] = "zset"
	}
	for k, ts := range snap.Expires {
		store.expires[k] = time.Unix(ts, 0)
	}

	return nil
}

// StartRDBLoop saves an RDB snapshot every interval.
func StartRDBLoop(path string, interval time.Duration) {
	go func() {
		for {
			time.Sleep(interval)
			if err := SaveRDB(path); err != nil {
				fmt.Println("RDB save error:", err)
			} else {
				fmt.Println("RDB snapshot saved")
			}
		}
	}()
}
