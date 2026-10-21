package main

import (
	"cinderdb/internal/aof"
	"cinderdb/internal/command"
	"cinderdb/internal/server"
	"cinderdb/internal/store"
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"
)

type config struct {
	addr, aofPath, policy   string
	timeout, expiryInterval time.Duration
	maxClients              int
}

func run(ctx context.Context, c config) (result error) {
	if c.timeout <= 0 || c.expiryInterval <= 0 || c.maxClients <= 0 {
		return fmt.Errorf("timeout, expiry interval and max clients must be positive")
	}
	if c.policy != "always" && c.policy != "everysec" && c.policy != "no" {
		return fmt.Errorf("invalid fsync policy")
	}
	data := store.New()
	if c.aofPath != "" {
		journal, err := aof.Open(c.aofPath, c.policy, data)
		if err != nil {
			return err
		}
		data.SetJournal(journal)
		defer func() { result = errors.Join(result, journal.Close()) }()
	}
	listener, err := net.Listen("tcp", c.addr)
	if err != nil {
		return err
	}
	log.Printf("CinderDB listening on %s", listener.Addr())
	srv := server.New(command.New(data))
	srv.Timeout = c.timeout
	srv.MaxClients = c.maxClients
	workerCtx, cancel := context.WithCancel(context.Background())
	workerDone := make(chan struct{})
	go func() { data.ExpireLoop(workerCtx, c.expiryInterval); close(workerDone) }()
	done := make(chan error, 1)
	go func() { done <- srv.Serve(listener) }()
	received := false
	select {
	case <-ctx.Done():
	case err = <-done:
		received = true
	}
	log.Print("CinderDB shutting down")
	srv.Close()
	if !received {
		err = <-done
	}
	cancel()
	<-workerDone
	if err != nil && !errors.Is(err, net.ErrClosed) {
		return err
	}
	return nil
}
func main() {
	c := config{}
	flag.StringVar(&c.addr, "addr", "127.0.0.1:6379", "TCP listen address")
	flag.StringVar(&c.aofPath, "aof", "", "AOF path (empty disables persistence)")
	flag.StringVar(&c.policy, "fsync", "everysec", "always, everysec or no")
	flag.DurationVar(&c.timeout, "timeout", 30*time.Second, "per-command socket deadline")
	flag.DurationVar(&c.expiryInterval, "expiry-interval", 100*time.Millisecond, "active expiry scan interval")
	flag.IntVar(&c.maxClients, "max-clients", 128, "maximum concurrent connections")
	flag.Parse()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, c); err != nil {
		log.Fatal(err)
	}
}
