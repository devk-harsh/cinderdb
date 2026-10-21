package command

import (
	"cinderdb/internal/resp"
	"cinderdb/internal/store"
	"sort"
	"strconv"
)

var wrongType = resp.Error("WRONGTYPE Operation against a key holding the wrong kind of value")
var wrongArity = resp.Error("ERR wrong number of arguments")

func lists(tx *store.Tx, cmd string, a []string) resp.Value {
	if len(a) < 1 {
		return wrongArity
	}
	switch cmd {
	case "LPUSH", "RPUSH":
		if len(a) < 2 {
			return wrongArity
		}
	case "LRANGE":
		if len(a) != 3 {
			return wrongArity
		}
	default:
		if len(a) != 1 {
			return wrongArity
		}
	}
	var start, end int64
	if cmd == "LRANGE" {
		var e error
		start, e = strconv.ParseInt(a[1], 10, 64)
		if e != nil {
			return resp.Error("ERR value is not an integer or out of range")
		}
		end, e = strconv.ParseInt(a[2], 10, 64)
		if e != nil {
			return resp.Error("ERR value is not an integer or out of range")
		}
	}
	v := tx.Get(a[0])
	if v != nil && v.Kind != "list" {
		return wrongType
	}
	if cmd == "LPUSH" || cmd == "RPUSH" {
		if v != nil {
			v = tx.Edit(a[0])
		}
		if v == nil {
			v = &store.Entry{Kind: "list"}
			tx.Put(a[0], v)
		}
		if cmd == "LPUSH" {
			prefix := make([]string, len(a)-1)
			for i := 1; i < len(a); i++ {
				prefix[len(a)-1-i] = a[i]
			}
			v.List = append(prefix, v.List...)
		} else {
			v.List = append(v.List, a[1:]...)
		}
		return resp.Integer(int64(len(v.List)))
	}
	if cmd == "LLEN" {
		if v == nil {
			return resp.Integer(0)
		}
		return resp.Integer(int64(len(v.List)))
	}
	if cmd == "LRANGE" {
		if v == nil {
			return resp.Strings(nil)
		}
		n := int64(len(v.List))
		if start < 0 {
			start += n
		}
		if end < 0 {
			end += n
		}
		if start < 0 {
			start = 0
		}
		if end >= n {
			end = n - 1
		}
		if start >= n || end < 0 || start > end {
			return resp.Strings(nil)
		}
		return resp.Strings(v.List[int(start) : int(end)+1])
	}
	if v == nil {
		return resp.Null()
	}
	v = tx.Edit(a[0])
	idx := 0
	if cmd == "RPOP" {
		idx = len(v.List) - 1
	}
	s := v.List[idx]
	// Clear removed references so popped values do not retain large strings.
	v.List[idx] = ""
	if idx == 0 {
		v.List = v.List[1:]
	} else {
		v.List = v.List[:idx]
	}
	if len(v.List) == 0 {
		tx.Delete(a[0])
	}
	return resp.Bulk(s)
}

func sets(tx *store.Tx, cmd string, a []string) resp.Value {
	if len(a) < 1 {
		return wrongArity
	}
	switch cmd {
	case "SADD", "SREM":
		if len(a) < 2 {
			return wrongArity
		}
	case "SISMEMBER":
		if len(a) != 2 {
			return wrongArity
		}
	default:
		if len(a) != 1 {
			return wrongArity
		}
	}
	v := tx.Get(a[0])
	if v != nil && v.Kind != "set" {
		return wrongType
	}
	if v == nil {
		if cmd == "SMEMBERS" {
			return resp.Strings(nil)
		}
		if cmd != "SADD" {
			return resp.Integer(0)
		}
		v = &store.Entry{Kind: "set", Set: make(map[string]struct{})}
		tx.Put(a[0], v)
	}
	switch cmd {
	case "SADD", "SREM":
		v = tx.Edit(a[0])
		n := int64(0)
		for _, s := range a[1:] {
			_, ok := v.Set[s]
			if cmd == "SADD" && !ok {
				v.Set[s] = struct{}{}
				n++
			}
			if cmd == "SREM" && ok {
				delete(v.Set, s)
				n++
			}
		}
		if len(v.Set) == 0 {
			tx.Delete(a[0])
		}
		return resp.Integer(n)
	case "SISMEMBER":
		if _, ok := v.Set[a[1]]; ok {
			return resp.Integer(1)
		}
		return resp.Integer(0)
	case "SCARD":
		return resp.Integer(int64(len(v.Set)))
	default:
		ss := make([]string, 0, len(v.Set))
		for s := range v.Set {
			ss = append(ss, s)
		}
		sort.Strings(ss)
		return resp.Strings(ss)
	}
}

func hashes(tx *store.Tx, cmd string, a []string) resp.Value {
	if len(a) < 1 {
		return wrongArity
	}
	switch cmd {
	case "HSET":
		if len(a) < 3 || len(a)%2 != 1 {
			return wrongArity
		}
	case "HDEL":
		if len(a) < 2 {
			return wrongArity
		}
	case "HGET", "HEXISTS":
		if len(a) != 2 {
			return wrongArity
		}
	default:
		if len(a) != 1 {
			return wrongArity
		}
	}
	v := tx.Get(a[0])
	if v != nil && v.Kind != "hash" {
		return wrongType
	}
	if v == nil {
		if cmd == "HGET" {
			return resp.Null()
		}
		if cmd == "HGETALL" {
			return resp.Strings(nil)
		}
		if cmd != "HSET" {
			return resp.Integer(0)
		}
		v = &store.Entry{Kind: "hash", Hash: make(map[string]string)}
		tx.Put(a[0], v)
	}
	switch cmd {
	case "HSET":
		v = tx.Edit(a[0])
		n := int64(0)
		for i := 1; i < len(a); i += 2 {
			if _, ok := v.Hash[a[i]]; !ok {
				n++
			}
			v.Hash[a[i]] = a[i+1]
		}
		return resp.Integer(n)
	case "HDEL":
		v = tx.Edit(a[0])
		n := int64(0)
		for _, s := range a[1:] {
			if _, ok := v.Hash[s]; ok {
				delete(v.Hash, s)
				n++
			}
		}
		if len(v.Hash) == 0 {
			tx.Delete(a[0])
		}
		return resp.Integer(n)
	case "HGET", "HEXISTS":
		s, ok := v.Hash[a[1]]
		if cmd == "HEXISTS" {
			if ok {
				return resp.Integer(1)
			}
			return resp.Integer(0)
		}
		if !ok {
			return resp.Null()
		}
		return resp.Bulk(s)
	default:
		keys := make([]string, 0, len(v.Hash))
		for k := range v.Hash {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		ss := make([]string, 0, 2*len(keys))
		for _, k := range keys {
			ss = append(ss, k, v.Hash[k])
		}
		return resp.Strings(ss)
	}
}
