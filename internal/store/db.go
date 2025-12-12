package store

// DB is the keyspace: the analog of redisDb (db.c) with Go's built-in map
// standing in for dict.c (incremental rehashing is a later, optional
// fidelity upgrade). Owned exclusively by the event-loop goroutine — that
// ownership is the entire concurrency story, so there are deliberately no
// locks here.
type DB struct {
	dict map[string]*Object

	// Keyspace stats (server.stat_keyspace_hits/misses). Counted only on
	// read-path lookups, mirroring lookupKeyReadWithFlags.
	Hits, Misses uint64
}

func NewDB() *DB {
	return &DB{dict: make(map[string]*Object)}
}

// LookupRead finds a key for a read command (GET, EXISTS, TYPE, OBJECT) and
// maintains the hit/miss counters (lookupKeyRead).
func (db *DB) LookupRead(key []byte) *Object {
	o := db.dict[string(key)] // string(key) in a map index does not allocate
	if o != nil {
		db.Hits++
	} else {
		db.Misses++
	}
	return o
}

// LookupWrite finds a key on behalf of a mutating command (lookupKeyWrite):
// same lookup, but write-path lookups do not touch the hit/miss stats.
func (db *DB) LookupWrite(key []byte) *Object {
	return db.dict[string(key)]
}

// Set adds or overwrites a key (genericSetKey).
// TODO(ttl): when expiration lands (Phase 3), a plain overwrite must clear
// any TTL on the key — Redis semantics for SET without KEEPTTL.
func (db *DB) Set(key []byte, o *Object) {
	db.dict[string(key)] = o
}

// Delete removes a key, reporting whether it existed (dbDelete).
func (db *DB) Delete(key []byte) bool {
	if _, ok := db.dict[string(key)]; !ok {
		return false
	}
	delete(db.dict, string(key))
	return true
}

// Keys returns all keys matching the glob pattern (keysCommand). Like Redis,
// this walks the entire keyspace on the event-loop thread — O(N) latency by
// design; don't parallelize it, that would break single-owner.
func (db *DB) Keys(pattern []byte) [][]byte {
	allKeys := len(pattern) == 1 && pattern[0] == '*'
	out := make([][]byte, 0, 16)
	for k := range db.dict {
		if allKeys || Match(pattern, []byte(k)) {
			out = append(out, []byte(k))
		}
	}
	return out
}

// Len is the number of keys (dbSize).
func (db *DB) Len() int { return len(db.dict) }
