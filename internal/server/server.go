package server

import (
	"bufio"
	"cinderdb/internal/command"
	"cinderdb/internal/resp"
	"io"
	"net"
	"strings"
	"sync"
	"time"
)

type Server struct {
	Timeout    time.Duration
	MaxClients int
	Engine     *command.Engine
	mu         sync.Mutex
	listener   net.Listener
	clients    map[net.Conn]struct{}
	closing    bool
	wg         sync.WaitGroup
	once       sync.Once
}

func New(e *command.Engine) *Server {
	return &Server{Engine: e, clients: make(map[net.Conn]struct{}), Timeout: 30 * time.Second, MaxClients: 128}
}
func (s *Server) Handle(conn net.Conn) {
	defer conn.Close()
	d := resp.NewDecoder(conn)
	w := bufio.NewWriter(conn)
	for {
		if s.Timeout > 0 {
			conn.SetDeadline(time.Now().Add(s.Timeout))
		}
		v, err := d.Decode()
		if err != nil {
			if err != io.EOF {
				resp.Encode(w, resp.Error("ERR protocol error"))
				w.Flush()
			}
			return
		}
		reply := s.Engine.Execute(v)
		if resp.Encode(w, reply) != nil || w.Flush() != nil {
			return
		}
		if reply.Kind == '+' && v.Kind == '*' && len(v.Array) == 1 && strings.EqualFold(v.Array[0].Text, "QUIT") {
			return
		}
	}
}
func (s *Server) Serve(listener net.Listener) error {
	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		listener.Close()
		return net.ErrClosed
	}
	s.listener = listener
	s.mu.Unlock()
	for {
		conn, err := listener.Accept()
		if err != nil {
			return err
		}
		s.mu.Lock()
		if s.closing {
			s.mu.Unlock()
			conn.Close()
			return net.ErrClosed
		}
		if s.MaxClients > 0 && len(s.clients) >= s.MaxClients {
			s.mu.Unlock()
			conn.Close()
			continue
		}
		s.clients[conn] = struct{}{}
		// Add while holding mu so Close cannot begin Wait before registration.
		s.wg.Add(1)
		s.mu.Unlock()
		go func() {
			defer s.wg.Done()
			defer func() { s.mu.Lock(); delete(s.clients, conn); s.mu.Unlock() }()
			s.Handle(conn)
		}()
	}
}
func (s *Server) Close() {
	s.once.Do(func() {
		s.mu.Lock()
		s.closing = true
		if s.listener != nil {
			s.listener.Close()
		}
		for c := range s.clients {
			c.Close()
		}
		s.mu.Unlock()
		s.wg.Wait()
	})
}
