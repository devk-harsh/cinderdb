# CinderDB

An in-memory key-value database written in Go. It uses RESP2 over TCP and supports strings, lists, sets, hashes, key expiration, and atomic counters.

Requires Go 1.23 or newer. There are no external dependencies.

Run from the project directory:

```sh
go run ./cmd/cinderdb
```

The server listens on `127.0.0.1:6379`. To save data between restarts:

```sh
go run ./cmd/cinderdb -aof cinderdb.aof
```

Connect with `redis-cli -p 6379`:

```text
SET name cinder
GET name
INCR counter
SET session active EX 60
```

Use `-addr` to change the listen address. Persistence defaults to `-fsync everysec`; `always` and `no` are also supported. Press Ctrl+C to stop the server.

Only a subset of Redis commands is supported. The persistence format is specific to CinderDB, and authentication is not implemented.
