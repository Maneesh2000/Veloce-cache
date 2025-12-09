// Package store is the pure data layer: the object model (Redis object.c),
// the keyspace (db.c), and supporting utilities (util.c). It has no
// dependency on the wire protocol or the server; only the event-loop
// goroutine may touch it — that single-owner rule is what makes the whole
// package lock-free.
package store

import "math"

// ObjType is the user-visible value type (Redis OBJ_STRING, OBJ_LIST, ...).
// It drives command type-checking and the TYPE command.
type ObjType uint8

const (
	TypeString ObjType = iota
	// TypeList, TypeSet, TypeZSet, TypeHash arrive in Phase 4.
)

// Encoding is how Val is physically represented (Redis OBJ_ENCODING_*).
// It is a private optimization of the type, surfaced by OBJECT ENCODING.
type Encoding uint8

const (
	EncRaw    Encoding = iota // string: large payload
	EncInt                    // string: canonical integer, Val is int64
	EncEmbstr                 // string: short payload (<= EmbstrCutoff)
)

// EmbstrCutoff mirrors OBJ_ENCODING_EMBSTR_SIZE_LIMIT (44). In C it marks
// where the header+sds single-allocation trick stops paying; here it is kept
// purely so OBJECT ENCODING agrees with real Redis.
const EmbstrCutoff = 44

// SharedIntCount mirrors OBJ_SHARED_INTEGERS: values 0..9999 are served from
// a singleton table so hot small integers cost zero allocations.
const SharedIntCount = 10000

// Object is the Go analog of robj (redisObject). Deliberate deltas from C:
// no refcount (GC frees; sharing is guarded by pointer identity instead),
// and no 16-byte bit-packing (Go has no bitfields; we match the design, not
// the byte count).
type Object struct {
	Type     ObjType
	Encoding Encoding
	lruLfu   uint32 // reserved: LRU timestamp OR LFU counter+atime (Phase 7)
	Val      any    // int64 (EncInt) | []byte (EncRaw, EncEmbstr)
}

// sharedIntegers are the 0..9999 singletons (shared.integers in server.c).
// They must NEVER be mutated; IsShared is the guard writers check.
var sharedIntegers [SharedIntCount]Object

func init() {
	for i := range sharedIntegers {
		sharedIntegers[i] = Object{Type: TypeString, Encoding: EncInt, Val: int64(i)}
	}
}

// NewInt returns a string object holding v as a native integer — the shared
// singleton when 0 <= v < SharedIntCount (createStringObjectFromLongLong).
func NewInt(v int64) *Object {
	if v >= 0 && v < SharedIntCount {
		return &sharedIntegers[v]
	}
	return &Object{Type: TypeString, Encoding: EncInt, Val: v}
}

// IsShared reports whether o is one of the shared singletons and therefore
// must not be mutated in place (replaces C's refcount==OBJ_SHARED_REFCOUNT).
func (o *Object) IsShared() bool {
	if o.Encoding != EncInt {
		return false
	}
	v := o.Val.(int64)
	return v >= 0 && v < SharedIntCount && o == &sharedIntegers[v]
}

// TryEncode builds a string object from raw bytes, picking the most compact
// encoding (tryObjectEncoding): canonical integer of <= 20 chars -> int,
// short -> embstr, else raw. May retain b for embstr/raw (callers hand over
// ownership; the RESP parser already returns private copies).
func TryEncode(b []byte) *Object {
	if len(b) <= 20 {
		if v, ok := String2ll(b); ok {
			return NewInt(v)
		}
	}
	enc := EncRaw
	if len(b) <= EmbstrCutoff {
		enc = EncEmbstr
	}
	return &Object{Type: TypeString, Encoding: enc, Val: b}
}

// TypeName is the TYPE command's word for this object.
func (o *Object) TypeName() string {
	switch o.Type {
	case TypeString:
		return "string"
	default:
		return "unknown"
	}
}

// EncodingName is the OBJECT ENCODING word (strEncoding in object.c).
func (o *Object) EncodingName() string {
	switch o.Encoding {
	case EncRaw:
		return "raw"
	case EncInt:
		return "int"
	case EncEmbstr:
		return "embstr"
	default:
		return "unknown"
	}
}

// String2ll is an exact port of string2ll (util.c): parse b as a base-10
// int64 accepting ONLY the canonical form — no leading zeros ("007" fails,
// bare "0" passes), no '+', no whitespace, no trailing bytes, at most 20
// chars, overflow checked at every digit. The strictness is what guarantees
// int-encoded values round-trip to the identical byte string.
