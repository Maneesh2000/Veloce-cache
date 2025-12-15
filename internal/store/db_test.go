package store

import (
	"sort"
	"testing"
)

func TestDBSetLookupDelete(t *testing.T) {
	db := NewDB()

	if o := db.LookupRead([]byte("k")); o != nil {
		t.Fatal("lookup on empty db returned object")
	}
	db.Set([]byte("k"), TryEncode([]byte("v")))
	o := db.LookupRead([]byte("k"))
	if o == nil || string(o.Val.([]byte)) != "v" {
		t.Fatalf("lookup after set: %v", o)
	}
	if db.Len() != 1 {
		t.Fatalf("Len = %d, want 1", db.Len())
	}

	// Overwrite re-encodes.
	db.Set([]byte("k"), TryEncode([]byte("123")))
	if got := db.LookupRead([]byte("k")).Encoding; got != EncInt {
		t.Fatalf("overwrite encoding = %v, want EncInt", got)
	}

	if !db.Delete([]byte("k")) {
		t.Fatal("Delete existing returned false")
	}
	if db.Delete([]byte("k")) {
		t.Fatal("Delete missing returned true")
	}
	if db.Len() != 0 {
		t.Fatalf("Len after delete = %d", db.Len())
	}
}

func TestDBBinaryKeys(t *testing.T) {
	db := NewDB()
	key := []byte("bin\x00key\r\n")
	db.Set(key, TryEncode([]byte("v")))
	if db.LookupRead(key) == nil {
		t.Fatal("binary key not found")
	}
	if db.LookupRead([]byte("bin\x00key")) != nil {
		t.Fatal("prefix of binary key wrongly found")
	}
}

func TestDBHitMissAccounting(t *testing.T) {
	db := NewDB()
	db.Set([]byte("k"), TryEncode([]byte("v")))

	db.LookupRead([]byte("k"))       // hit
	db.LookupRead([]byte("missing")) // miss
	db.LookupRead([]byte("k"))       // hit

	// Write-path lookups must not count (Redis LOOKUP_WRITE behavior).
	db.LookupWrite([]byte("k"))
	db.LookupWrite([]byte("missing"))

	if db.Hits != 2 || db.Misses != 1 {
		t.Fatalf("hits=%d misses=%d, want 2/1", db.Hits, db.Misses)
	}
}

func TestDBKeys(t *testing.T) {
	db := NewDB()
	for _, k := range []string{"user:1", "user:2", "session:1", "u"} {
		db.Set([]byte(k), TryEncode([]byte("v")))
	}

	got := func(pattern string) []string {
		keys := db.Keys([]byte(pattern))
		out := make([]string, len(keys))
		for i, k := range keys {
			out[i] = string(k)
		}
		sort.Strings(out) // map order is random
		return out
	}

	if g := got("*"); len(g) != 4 {
		t.Fatalf("KEYS * = %v", g)
	}
	if g := got("user:*"); !(len(g) == 2 && g[0] == "user:1" && g[1] == "user:2") {
		t.Fatalf("KEYS user:* = %v", g)
	}
	if g := got("user:?"); len(g) != 2 {
		t.Fatalf("KEYS user:? = %v", g)
	}
	if g := got("nomatch*"); len(g) != 0 {
		t.Fatalf("KEYS nomatch* = %v", g)
	}
}
