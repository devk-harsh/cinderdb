package command

import (
	"cinderdb/internal/resp"
	"cinderdb/internal/store"
	"errors"
	"math"
	"strconv"
	"strings"
)

type Engine struct{ Store *store.Store }

func New(s *store.Store) *Engine { return &Engine{Store: s} }
func (e *Engine) Execute(v resp.Value) resp.Value {
	if v.Kind != '*' || v.Null || len(v.Array) == 0 || len(v.Array) > resp.MaxArray {
		return resp.Error("ERR command must be a nonempty array")
	}
	a := make([]string, len(v.Array))
	for i, x := range v.Array {
		if x.Kind != '$' || x.Null {
			return resp.Error("ERR command arguments must be bulk strings")
		}
		a[i] = x.Text
	}
	var result resp.Value
	err := e.Store.With(func(tx *store.Tx) { result = e.run(tx, a) })
	if err != nil {
		return resp.Error("MISCONF AOF persistence failure; restart required")
	}
	return result
}

func (e *Engine) run(tx *store.Tx, a []string) resp.Value {
	cmd := strings.ToUpper(a[0])
	args := a[1:]
	arity := func() resp.Value { return resp.Error("ERR wrong number of arguments") }
	switch cmd {
	case "PING":
		if len(args) == 0 {
			return resp.Simple("PONG")
		}
		if len(args) == 1 {
			return resp.Bulk(args[0])
		}
		return arity()
	case "ECHO":
		if len(args) != 1 {
			return arity()
		}
		return resp.Bulk(args[0])
	case "SET":
		return set(tx, args)
	case "INCR", "DECR":
		return counter(tx, cmd, args)
	case "GET":
		if len(args) != 1 {
			return arity()
		}
		s := tx.Get(args[0])
		if s == nil {
			return resp.Null()
		}
		if s.Kind != "string" {
			return wrongType
		}
		return resp.Bulk(s.Text)
	case "DEL", "EXISTS":
		if len(args) < 1 {
			return arity()
		}
		n := int64(0)
		for _, k := range args {
			if tx.Get(k) != nil {
				n++
				if cmd == "DEL" {
					tx.Delete(k)
				}
			}
		}
		return resp.Integer(n)
	case "EXPIRE", "PEXPIRE":
		if len(args) != 2 {
			return arity()
		}
		unit := int64(1)
		if cmd == "EXPIRE" {
			unit = 1000
		}
		at, err := deadline(args[1], unit, tx.Now(), false)
		if err != nil {
			return resp.Error("ERR invalid expire time")
		}
		v := tx.Get(args[0])
		if v == nil {
			return resp.Integer(0)
		}
		if at <= tx.Now() {
			tx.Delete(args[0])
		} else {
			tx.Edit(args[0]).ExpiresAt = at
		}
		return resp.Integer(1)
	case "TTL", "PTTL":
		if len(args) != 1 {
			return arity()
		}
		v := tx.Get(args[0])
		if v == nil {
			return resp.Integer(-2)
		}
		if v.ExpiresAt == 0 {
			return resp.Integer(-1)
		}
		n := v.ExpiresAt - tx.Now()
		if cmd == "TTL" {
			extra := int64(0)
			if n%1000 >= 500 {
				extra = 1
			}
			n = n/1000 + extra
		}
		return resp.Integer(n)
	case "PERSIST":
		if len(args) != 1 {
			return arity()
		}
		v := tx.Get(args[0])
		if v == nil || v.ExpiresAt == 0 {
			return resp.Integer(0)
		}
		tx.Edit(args[0]).ExpiresAt = 0
		return resp.Integer(1)
	case "LPUSH", "RPUSH", "LPOP", "RPOP", "LLEN", "LRANGE":
		return lists(tx, cmd, args)
	case "SADD", "SREM", "SISMEMBER", "SMEMBERS", "SCARD":
		return sets(tx, cmd, args)
	case "HSET", "HGET", "HDEL", "HEXISTS", "HGETALL":
		return hashes(tx, cmd, args)
	case "DBSIZE":
		if len(args) != 0 {
			return arity()
		}
		return resp.Integer(int64(len(tx.Keys())))
	case "FLUSHDB":
		if len(args) != 0 {
			return arity()
		}
		for _, k := range tx.Keys() {
			tx.Delete(k)
		}
		return resp.Simple("OK")
	case "QUIT":
		if len(args) != 0 {
			return arity()
		}
		return resp.Simple("OK")
	default:
		return resp.Error("ERR unknown command")
	}
}
func deadline(s string, unit, now int64, positive bool) (int64, error) {
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil || (positive && n <= 0) {
		return 0, fmtError
	}
	if n <= 0 {
		return now, nil
	}
	if n > (math.MaxInt64-now)/unit {
		return 0, fmtError
	}
	return now + n*unit, nil
}

var fmtError = errors.New("invalid integer or expiration")
