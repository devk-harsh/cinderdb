// Package resp implements RESP2 framing without assuming TCP packet boundaries.
package resp

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

const (
	MaxBulk  = 1 << 20
	MaxArray = 1024
	MaxDepth = 16
	MaxFrame = 4 << 20
	MaxLine  = 4096
)

var ErrProtocol = errors.New("RESP protocol error")

type Value struct {
	Kind  byte
	Text  string
	Int   int64
	Array []Value
	Null  bool
}

func Simple(s string) Value { return Value{Kind: '+', Text: s} }
func Error(s string) Value  { return Value{Kind: '-', Text: s} }
func Bulk(s string) Value   { return Value{Kind: '$', Text: s} }
func Integer(n int64) Value { return Value{Kind: ':', Int: n} }
func Null() Value           { return Value{Kind: '$', Null: true} }
func Strings(ss []string) Value {
	v := Value{Kind: '*', Array: make([]Value, len(ss))}
	for i, s := range ss {
		v.Array[i] = Bulk(s)
	}
	return v
}

type Decoder struct {
	r         *bufio.Reader
	remaining int
}

func NewDecoder(r io.Reader) *Decoder     { return &Decoder{r: bufio.NewReader(r)} }
func (d *Decoder) Decode() (Value, error) { d.remaining = MaxFrame; return d.decode(0) }
func (d *Decoder) byte() (byte, error) {
	if d.remaining <= 0 {
		return 0, ErrProtocol
	}
	b, e := d.r.ReadByte()
	if e == nil {
		d.remaining--
	}
	return b, e
}
func (d *Decoder) line() (string, error) {
	b := make([]byte, 0, 32)
	for len(b) <= MaxLine {
		c, e := d.byte()
		if e != nil {
			if e == io.EOF {
				e = io.ErrUnexpectedEOF
			}
			return "", e
		}
		if c == '\n' {
			if len(b) == 0 || b[len(b)-1] != '\r' {
				return "", ErrProtocol
			}
			return string(b[:len(b)-1]), nil
		}
		b = append(b, c)
	}
	return "", ErrProtocol
}

func parseNumber(s string) (int64, error) {
	if s == "" || s[0] == '+' {
		return 0, ErrProtocol
	}
	for i, c := range s {
		if c == '-' && i == 0 {
			continue
		}
		if c < '0' || c > '9' {
			return 0, ErrProtocol
		}
	}
	n, e := strconv.ParseInt(s, 10, 64)
	if e != nil {
		return 0, ErrProtocol
	}
	return n, nil
}

func (d *Decoder) decode(depth int) (Value, error) {
	if depth > MaxDepth {
		return Value{}, ErrProtocol
	}
	prefix, e := d.byte()
	if e != nil {
		return Value{}, e
	}
	if !strings.ContainsRune("+-:$*", rune(prefix)) {
		return Value{}, ErrProtocol
	}
	line, e := d.line()
	if e != nil {
		return Value{}, e
	}
	v := Value{Kind: prefix}
	if prefix == '+' || prefix == '-' {
		if strings.ContainsRune(line, '\r') {
			return Value{}, ErrProtocol
		}
		v.Text = line
		return v, nil
	}
	n, e := parseNumber(line)
	if e != nil {
		return Value{}, e
	}
	switch prefix {
	case ':':
		v.Int = n
	case '$':
		if n == -1 {
			v.Null = true
			return v, nil
		}
		if n < 0 || n > MaxBulk || n+2 > int64(d.remaining) {
			return Value{}, ErrProtocol
		}
		b := make([]byte, int(n)+2)
		if _, e := io.ReadFull(d.r, b); e != nil {
			if e == io.EOF {
				e = io.ErrUnexpectedEOF
			}
			return Value{}, e
		}
		d.remaining -= len(b)
		if b[len(b)-2] != '\r' || b[len(b)-1] != '\n' {
			return Value{}, ErrProtocol
		}
		v.Text = string(b[:len(b)-2])
	case '*':
		if n == -1 {
			v.Null = true
			return v, nil
		}
		if n < 0 || n > MaxArray {
			return Value{}, ErrProtocol
		}
		v.Array = make([]Value, 0, int(n))
		for i := int64(0); i < n; i++ {
			child, e := d.decode(depth + 1)
			if e != nil {
				if e == io.EOF {
					e = io.ErrUnexpectedEOF
				}
				return Value{}, e
			}
			v.Array = append(v.Array, child)
		}
	}
	return v, nil
}

func Encode(w io.Writer, v Value) error {
	var e error
	switch v.Kind {
	case '+', '-':
		if strings.ContainsAny(v.Text, "\r\n") {
			return ErrProtocol
		}
		_, e = fmt.Fprintf(w, "%c%s\r\n", v.Kind, v.Text)
	case ':':
		_, e = fmt.Fprintf(w, ":%d\r\n", v.Int)
	case '$':
		if v.Null {
			_, e = io.WriteString(w, "$-1\r\n")
		} else {
			_, e = fmt.Fprintf(w, "$%d\r\n%s\r\n", len(v.Text), v.Text)
		}
	case '*':
		if v.Null {
			_, e = io.WriteString(w, "*-1\r\n")
			return e
		}
		_, e = fmt.Fprintf(w, "*%d\r\n", len(v.Array))
		if e != nil {
			return e
		}
		for _, child := range v.Array {
			if e = Encode(w, child); e != nil {
				return e
			}
		}
	default:
		e = ErrProtocol
	}
	return e
}
