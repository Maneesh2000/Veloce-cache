package server

import (
	"math"

	"github.com/Maneesh2000/Veloce-cache/internal/resp"
	"github.com/Maneesh2000/Veloce-cache/internal/store"
)

// String commands — the t_string.c analog.

const (
	wrongTypeErr    = "WRONGTYPE Operation against a key holding the wrong kind of value"
	notAnIntegerErr = "ERR value is not an integer or out of range"
	incrOverflowErr = "ERR increment or decrement would overflow"
)

// addReplyBulkObj renders a string object as a bulk reply. Int-encoded
// values are formatted straight into the output buffer — no intermediate
// object, mirroring how Redis serves shared integers without allocation.
func addReplyBulkObj(c *client, o *store.Object) {
	if o.Encoding == store.EncInt {
		c.out = resp.AppendBulkInt64(c.out, o.Val.(int64))
	} else {
		c.out = resp.AppendBulk(c.out, o.Val.([]byte))
	}
}

// checkTypeString replies WRONGTYPE and returns false unless o is a string.
// (Unreachable until Phase 4 introduces other types, but the guard belongs
// in every string command now — checkType in Redis.)
func checkTypeString(c *client, o *store.Object) bool {
	if o.Type != store.TypeString {
		c.out = resp.AppendError(c.out, wrongTypeErr)
		return false
	}
	return true
}

// setCommand: plain SET key value (setGenericCommand without options).
// Options (NX/XX/EX/PX/KEEPTTL/GET) come with later phases; until then any
// extra argument is a syntax error, like an unknown option in Redis.
// TODO(ttl): when expiration lands, plain SET must clear an existing TTL.
func setCommand(s *Server, c *client, args [][]byte) {
	if len(args) > 3 {
		c.out = resp.AppendError(c.out, "ERR syntax error")
		return
	}
	s.db.Set(args[1], store.TryEncode(args[2]))
	c.out = resp.AppendSimpleString(c.out, "OK")
}

// getCommand: GET key (getGenericCommand).
func getCommand(s *Server, c *client, args [][]byte) {
	o := s.db.LookupRead(args[1])
	if o == nil {
		c.out = resp.AppendNull(c.out)
		return
	}
	if !checkTypeString(c, o) {
		return
	}
	addReplyBulkObj(c, o)
}

// getInt64FromObject extracts the integer value of a string object under
// INCR semantics (getLongLongFromObject): int encoding reads directly,
// otherwise the payload must be a canonical string2ll integer.
func getInt64FromObject(o *store.Object) (int64, bool) {
	if o.Type != store.TypeString {
		return 0, false
	}
	if o.Encoding == store.EncInt {
		return o.Val.(int64), true
	}
	return store.String2ll(o.Val.([]byte))
}

// incrDecr implements INCR/DECR/INCRBY/DECRBY (incrDecrCommand).
func incrDecr(s *Server, c *client, key []byte, incr int64) {
	o := s.db.LookupWrite(key)
	var oldValue int64
	if o != nil {
		if !checkTypeString(c, o) {
			return
		}
		v, ok := getInt64FromObject(o)
		if !ok {
			c.out = resp.AppendError(c.out, notAnIntegerErr)
			return
		}
		oldValue = v
	} // missing key behaves as 0

	// Overflow pre-check, exactly as in incrDecrCommand.
	if (incr < 0 && oldValue < 0 && incr < math.MinInt64-oldValue) ||
		(incr > 0 && oldValue > 0 && incr > math.MaxInt64-oldValue) {
		c.out = resp.AppendError(c.out, incrOverflowErr)
		return
	}
	value := oldValue + incr

	// In-place mutation is allowed only for a private (non-shared)
	// int-encoded object; otherwise install a new object (which NewInt
	// serves from the shared table when in range).
	if o != nil && o.Encoding == store.EncInt && !o.IsShared() {
		o.Val = value
	} else {
		s.db.Set(key, store.NewInt(value))
	}
	c.out = resp.AppendInt(c.out, value)
}

func incrCommand(s *Server, c *client, args [][]byte) {
	incrDecr(s, c, args[1], 1)
}

func decrCommand(s *Server, c *client, args [][]byte) {
	incrDecr(s, c, args[1], -1)
}

func incrbyCommand(s *Server, c *client, args [][]byte) {
	incr, ok := store.String2ll(args[2])
	if !ok {
		c.out = resp.AppendError(c.out, notAnIntegerErr)
		return
	}
	incrDecr(s, c, args[1], incr)
}

func decrbyCommand(s *Server, c *client, args [][]byte) {
	decr, ok := store.String2ll(args[2])
	if !ok {
		c.out = resp.AppendError(c.out, notAnIntegerErr)
		return
	}
	// Negating MinInt64 overflows; Redis rejects it up front with a
	// dedicated message (decrbyCommand, t_string.c).
	if decr == math.MinInt64 {
		c.out = resp.AppendError(c.out, "ERR decrement would overflow")
		return
	}
	incrDecr(s, c, args[1], -decr)
}
