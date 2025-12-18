// Veloce-cache — a Redis server built from scratch in Go on a hand-rolled
// epoll/kqueue event loop. Phases 1-2: reactor + RESP2 + key-value core.
package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/Maneesh2000/Veloce-cache/internal/server"
)

func main() {
	bind := flag.String("bind", "127.0.0.1", "IPv4 address to listen on")
	port := flag.Int("port", 6379, "TCP port to listen on")
	flag.Parse()

	logger := log.New(os.Stderr, "veloce ", log.LstdFlags)

	srv, err := server.New(*bind, *port)
	if err != nil {
		logger.Fatalf("startup failed: %v", err)
	}
	srv.Logger = logger

	// Graceful-ish stop on SIGINT/SIGTERM (full graceful shutdown with AOF
	// flush is Phase 8; here we just exit the loop cleanly).
	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigc
		logger.Printf("received %s, shutting down", sig)
		srv.Stop()
	}()

	logger.Printf("ready to accept connections at %s (pid %d)", srv.Addr(), os.Getpid())
	if err := srv.Serve(); err != nil {
		logger.Fatalf("event loop error: %v", err)
	}
	logger.Printf("bye")
}
