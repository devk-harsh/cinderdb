// Package aof implements CinderDB's versioned, checksummed mutation journal.
// It is deliberately not compatible with Redis's on-disk AOF format.
package aof

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"os"
	"sort"
	"sync"
	"time"

	"cinderdb/internal/store"
)

const magic = "CDBAOF1\n"
const MaxRecord = 64 << 20

type pair struct {
	Field []byte
	Value []byte
}
type diskEntry struct {
	Kind      string
	Text      []byte
	List      [][]byte
	Set       [][]byte
	Hash      []pair
	ExpiresAt int64
}
type record struct {
	Key   []byte
	Value *diskEntry
}
type Log struct {
	mu       sync.Mutex
	file     *os.File
	policy   string
	fault    error
	stop     chan struct{}
	done     chan struct{}
	once     sync.Once
	closeErr error
}

func Open(path, policy string, s *store.Store) (*Log, error) {
	if policy != "always" && policy != "everysec" && policy != "no" {
		return nil, errors.New("fsync must be always, everysec or no")
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	l := &Log{file: f, policy: policy, stop: make(chan struct{}), done: make(chan struct{})}
	if err = l.replay(s); err != nil {
		f.Close()
		return nil, err
	}
	go l.syncLoop()
	return l, nil
}

func toDisk(v *store.Entry) *diskEntry {
	if v == nil {
		return nil
	}
	d := &diskEntry{Kind: v.Kind, Text: []byte(v.Text), ExpiresAt: v.ExpiresAt}
	for _, s := range v.List {
		d.List = append(d.List, []byte(s))
	}
	keys := make([]string, 0, len(v.Set))
	for k := range v.Set {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		d.Set = append(d.Set, []byte(k))
	}
	keys = keys[:0]
	for k := range v.Hash {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		d.Hash = append(d.Hash, pair{[]byte(k), []byte(v.Hash[k])})
	}
	return d
}

func fromDisk(d *diskEntry) (*store.Entry, error) {
	if d == nil {
		return nil, nil
	}
	if d.ExpiresAt < 0 {
		return nil, errors.New("negative expiration")
	}
	v := &store.Entry{Kind: d.Kind, Text: string(d.Text), ExpiresAt: d.ExpiresAt}
	switch d.Kind {
	case "string":
		if len(d.List)+len(d.Set)+len(d.Hash) != 0 {
			return nil, errors.New("invalid string fields")
		}
	case "list":
		if len(d.List) == 0 || len(d.Text)+len(d.Set)+len(d.Hash) != 0 {
			return nil, errors.New("invalid list fields")
		}
		for _, s := range d.List {
			v.List = append(v.List, string(s))
		}
	case "set":
		if len(d.Set) == 0 || len(d.Text)+len(d.List)+len(d.Hash) != 0 {
			return nil, errors.New("invalid set fields")
		}
		v.Set = make(map[string]struct{})
		for _, s := range d.Set {
			k := string(s)
			if _, ok := v.Set[k]; ok {
				return nil, errors.New("duplicate set member")
			}
			v.Set[k] = struct{}{}
		}
	case "hash":
		if len(d.Hash) == 0 || len(d.Text)+len(d.List)+len(d.Set) != 0 {
			return nil, errors.New("invalid hash fields")
		}
		v.Hash = make(map[string]string)
		for _, p := range d.Hash {
			k := string(p.Field)
			if _, ok := v.Hash[k]; ok {
				return nil, errors.New("duplicate hash field")
			}
			v.Hash[k] = string(p.Value)
		}
	default:
		return nil, errors.New("unknown entry kind")
	}
	return v, nil
}

func (l *Log) replay(s *store.Store) error {
	stat, err := l.file.Stat()
	if err != nil {
		return err
	}
	if stat.Size() == 0 {
		_, err = io.WriteString(l.file, magic)
		return err
	}
	header := make([]byte, len(magic))
	if _, err = io.ReadFull(l.file, header); err != nil || string(header) != magic {
		return errors.New("invalid AOF header")
	}
	count := 0
	for {
		var h [8]byte
		_, err := io.ReadFull(l.file, h[:])
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("truncated AOF record %d header: %w", count, err)
		}
		n := binary.BigEndian.Uint32(h[:4])
		if n == 0 || n > MaxRecord {
			return errors.New("invalid AOF record length")
		}
		b := make([]byte, int(n))
		if _, err = io.ReadFull(l.file, b); err != nil {
			return fmt.Errorf("truncated AOF record %d: %w", count, err)
		}
		if crc32.ChecksumIEEE(b) != binary.BigEndian.Uint32(h[4:]) {
			return fmt.Errorf("AOF checksum mismatch at record %d", count)
		}
		var rows []record
		decoder := json.NewDecoder(bytes.NewReader(b))
		decoder.DisallowUnknownFields()
		if err = decoder.Decode(&rows); err != nil {
			return fmt.Errorf("invalid AOF JSON: %w", err)
		}
		if len(rows) == 0 {
			return errors.New("empty AOF transaction")
		}
		if decoder.Decode(new(any)) != io.EOF {
			return errors.New("trailing AOF JSON")
		}
		changes := make([]store.Change, 0, len(rows))
		seen := make(map[string]bool)
		for _, r := range rows {
			k := string(r.Key)
			if seen[k] {
				return errors.New("duplicate AOF key")
			}
			seen[k] = true
			v, err := fromDisk(r.Value)
			if err != nil {
				return fmt.Errorf("invalid AOF entry: %w", err)
			}
			changes = append(changes, store.Change{Key: k, Value: v})
		}
		// Replay raw poststates in order; only expire after the entire log is loaded.
		if err = s.With(func(tx *store.Tx) {
			for _, c := range changes {
				if c.Value == nil {
					tx.Delete(c.Key)
				} else {
					tx.Put(c.Key, c.Value)
				}
			}
		}); err != nil {
			return err
		}
		count++
	}
	if err = s.With(func(tx *store.Tx) { tx.Keys() }); err != nil {
		return err
	}
	log.Printf("AOF recovered %d records", count)
	return nil
}

func (l *Log) Append(changes []store.Change) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.fault != nil {
		return l.fault
	}
	if len(changes) == 0 {
		return nil
	}
	rows := make([]record, 0, len(changes))
	for _, c := range changes {
		rows = append(rows, record{[]byte(c.Key), toDisk(c.Value)})
	}
	b, err := json.Marshal(rows)
	if err == nil && len(b) > MaxRecord {
		err = errors.New("AOF record exceeds 64 MiB")
	}
	if err == nil {
		var h [8]byte
		binary.BigEndian.PutUint32(h[:4], uint32(len(b)))
		binary.BigEndian.PutUint32(h[4:], crc32.ChecksumIEEE(b))
		var n int
		n, err = l.file.Write(h[:])
		if err == nil && n != len(h) {
			err = io.ErrShortWrite
		}
		if err == nil {
			n, err = l.file.Write(b)
			if err == nil && n != len(b) {
				err = io.ErrShortWrite
			}
		}
	}
	if err == nil && l.policy == "always" {
		err = l.file.Sync()
	}
	if err != nil {
		l.fault = err
		log.Printf("AOF persistence failure: %v", err)
	}
	return err
}

func (l *Log) syncLoop() {
	defer close(l.done)
	if l.policy != "everysec" {
		<-l.stop
		return
	}
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-l.stop:
			return
		case <-ticker.C:
			l.mu.Lock()
			if l.fault == nil {
				if err := l.file.Sync(); err != nil {
					l.fault = err
					log.Printf("AOF periodic fsync failure: %v", err)
				}
			}
			l.mu.Unlock()
		}
	}
}
func (l *Log) Close() error {
	l.once.Do(func() {
		close(l.stop)
		<-l.done
		l.mu.Lock()
		defer l.mu.Unlock()
		err := l.file.Sync()
		closeErr := l.file.Close()
		l.closeErr = errors.Join(l.fault, err, closeErr)
		l.fault = errors.New("AOF closed")
	})
	return l.closeErr
}
