package server

import "time"

// Stats is the observability seed (Phase 1 cross-cutting thread). All fields
// are mutated only from the event-loop goroutine, so no atomics are needed —
// the same free ride the keyspace gets from single-threaded execution.
// Snapshot() is provided for tests and, later, INFO.
type Stats struct {
	StartTime time.Time

	TotalConnectionsReceived uint64 // lifetime accepts
	ConnectedClients         int    // current
	RejectedConnections      uint64 // accept-time failures

	TotalCommandsProcessed uint64
	UnknownCommands        uint64
	ProtocolErrors         uint64

	TotalNetInputBytes  uint64
	TotalNetOutputBytes uint64

	// Keyspace counters. Live values are owned by store.DB (the loop
	// goroutine increments them inside LookupRead); these fields are filled
	// in by Server.Stats() when snapshotting.
	KeyspaceHits   uint64
	KeyspaceMisses uint64

	CommandCalls map[string]uint64 // per-command counters (commandstats)
}

func newStats() Stats {
	return Stats{
		StartTime:    time.Now(),
		CommandCalls: make(map[string]uint64),
	}
}

// snapshot returns a copy safe to read from another goroutine after the
// server has stopped, or approximately-correct while it runs (tests only).
func (s *Stats) snapshot() Stats {
	cp := *s
	cp.CommandCalls = make(map[string]uint64, len(s.CommandCalls))
	for k, v := range s.CommandCalls {
		cp.CommandCalls[k] = v
	}
	return cp
}
