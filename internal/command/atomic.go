package command

import (
	"cinderdb/internal/resp"
	"cinderdb/internal/store"
	"math"
	"strconv"
	"strings"
)

func set(tx *store.Tx, a []string) resp.Value {
	if len(a) < 2 {
		return wrongArity
	}
	nx, xx := false, false
	at := int64(0)
	for i := 2; i < len(a); i++ {
		switch strings.ToUpper(a[i]) {
		case "NX":
			if nx || xx {
				return resp.Error("ERR syntax error")
			}
			nx = true
		case "XX":
			if nx || xx {
				return resp.Error("ERR syntax error")
			}
			xx = true
		case "EX", "PX":
			if at != 0 || i+1 == len(a) {
				return resp.Error("ERR syntax error")
			}
			unit := int64(1)
			if strings.EqualFold(a[i], "EX") {
				unit = 1000
			}
			i++
			var err error
			at, err = deadline(a[i], unit, tx.Now(), true)
			if err != nil {
				return resp.Error("ERR invalid expire time in SET")
			}
		default:
			return resp.Error("ERR syntax error")
		}
	}
	v := tx.Get(a[0])
	if (nx && v != nil) || (xx && v == nil) {
		return resp.Null()
	}
	tx.Put(a[0], &store.Entry{Kind: "string", Text: a[1], ExpiresAt: at})
	return resp.Simple("OK")
}

func counter(tx *store.Tx, cmd string, a []string) resp.Value {
	if len(a) != 1 {
		return wrongArity
	}
	v := tx.Get(a[0])
	n := int64(0)
	at := int64(0)
	if v != nil {
		if v.Kind != "string" {
			return wrongType
		}
		var err error
		n, err = strconv.ParseInt(v.Text, 10, 64)
		// Redis integer values use canonical decimal syntax.
		if err != nil || strconv.FormatInt(n, 10) != v.Text {
			return resp.Error("ERR value is not an integer or out of range")
		}
		at = v.ExpiresAt
	}
	if (cmd == "INCR" && n == math.MaxInt64) || (cmd == "DECR" && n == math.MinInt64) {
		return resp.Error("ERR increment or decrement would overflow")
	}
	if cmd == "INCR" {
		n++
	} else {
		n--
	}
	// The surrounding Store.With holds the lock across lookup and replacement.
	tx.Put(a[0], &store.Entry{Kind: "string", Text: strconv.FormatInt(n, 10), ExpiresAt: at})
	return resp.Integer(n)
}
