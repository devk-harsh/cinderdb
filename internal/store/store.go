package store

import (
	"context"
	"sort"
	"sync"
	"time"
)

type Entry struct {
	Kind      string
	Text      string
	List      []string
	Set       map[string]struct{}
	Hash      map[string]string
	ExpiresAt int64
}
type Change struct {
	Key   string
	Value *Entry
}
type Journal interface{ Append([]Change) error }
type Store struct {
	mu      sync.Mutex
	entries map[string]*Entry
	now     func() time.Time
	journal Journal
	fault   error
}

// Tx references committed entries and stages replacements in pending.
// Mutable entries must be obtained with Edit, never modified through Get.
type Tx struct {
	entries map[string]*Entry
	pending map[string]*Entry
	now     int64
}

func New() *Store { return NewWithClock(time.Now) }
func NewWithClock(now func() time.Time) *Store {
	return &Store{entries: make(map[string]*Entry), now: now}
}
func (s *Store) SetJournal(j Journal) { s.mu.Lock(); defer s.mu.Unlock(); s.journal = j }

// With serializes validation, staged mutation, journal append and publication.
func (s *Store) With(fn func(*Tx)) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fault != nil {
		return s.fault
	}
	tx := &Tx{entries: s.entries, pending: make(map[string]*Entry), now: s.now().UnixMilli()}
	fn(tx)
	changes := make([]Change, 0, len(tx.pending))
	for k, v := range tx.pending {
		changes = append(changes, Change{Key: k, Value: v})
	}
	sort.Slice(changes, func(i, j int) bool { return changes[i].Key < changes[j].Key })
	// Even reads check the journal health after a periodic fsync failure.
	if s.journal != nil {
		if err := s.journal.Append(changes); err != nil {
			s.fault = err
			return err
		}
	}
	for _, c := range changes {
		if c.Value == nil {
			delete(s.entries, c.Key)
		} else {
			s.entries[c.Key] = c.Value
		}
	}
	return nil
}
func (t *Tx) Get(k string) *Entry {
	v, ok := t.pending[k]
	if !ok {
		v = t.entries[k]
	}
	if v != nil && v.ExpiresAt != 0 && v.ExpiresAt <= t.now {
		t.Delete(k)
		return nil
	}
	return v
}
func (t *Tx) Edit(k string) *Entry {
	v := t.Get(k)
	if v == nil {
		return nil
	}
	if _, ok := t.pending[k]; ok {
		return v
	}
	copy := *v
	copy.List = append([]string(nil), v.List...)
	if v.Set != nil {
		copy.Set = make(map[string]struct{}, len(v.Set))
		for k := range v.Set {
			copy.Set[k] = struct{}{}
		}
	}
	if v.Hash != nil {
		copy.Hash = make(map[string]string, len(v.Hash))
		for k, v := range v.Hash {
			copy.Hash[k] = v
		}
	}
	t.Put(k, &copy)
	return &copy
}
func (t *Tx) Put(k string, v *Entry) { t.pending[k] = v }
func (t *Tx) Delete(k string)        { t.pending[k] = nil }
func (t *Tx) Now() int64             { return t.now }
func (t *Tx) Keys() []string {
	seen := make(map[string]struct{}, len(t.entries)+len(t.pending))
	for k := range t.entries {
		seen[k] = struct{}{}
	}
	for k := range t.pending {
		seen[k] = struct{}{}
	}
	keys := make([]string, 0, len(seen))
	for k := range seen {
		if t.Get(k) != nil {
			keys = append(keys, k)
		}
	}
	return keys
}

// ExpireLoop uses one ticker for all keys; scans are deliberately simple O(N).
func (s *Store) ExpireLoop(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if s.With(func(t *Tx) { t.Keys() }) != nil {
				return
			}
		}
	}
}
