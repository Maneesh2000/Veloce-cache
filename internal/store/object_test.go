package store

import (
	"math"
	"strings"
	"testing"
)

func TestString2ll(t *testing.T) {
	accept := []struct {
		in   string
		want int64
	}{
		{"0", 0},
		{"1", 1},
		{"-1", -1},
		{"10", 10},
		{"9999", 9999},
		{"10000", 10000},
		{"9223372036854775807", math.MaxInt64},  // 19 digits
		{"-9223372036854775808", math.MinInt64}, // 20 chars incl. '-'
	}
	for _, tc := range accept {
		got, ok := String2ll([]byte(tc.in))
		if !ok || got != tc.want {
			t.Errorf("String2ll(%q) = (%d, %v), want (%d, true)", tc.in, got, ok, tc.want)
		}
	}

	reject := []string{
		"",                      // empty
		"-",                     // lone sign
		"+1",                    // Redis string2ll has no '+' support
		" 1",                    // leading space
		"1 ",                    // trailing space
		"1a",                    // trailing garbage
		"a1",                    // leading garbage
		"007",                   // leading zeros: not canonical
		"01",                    // leading zero
		"-0",                    // negative zero: first digit must be 1-9
		"-01",                   // ditto
		"1.5",                   // not an integer
		"0x10",                  // no hex
		"9223372036854775808",   // MaxInt64 + 1
		"-9223372036854775809",  // MinInt64 - 1
		"18446744073709551615",  // MaxUint64 (20 digits, overflows signed)
		"18446744073709551616",  // 2^64 (overflows the *10 check)
		"99999999999999999999",  // 20 digits, overflows
		"123456789012345678901", // 21 chars: length-rejected
		strings.Repeat("9", 30), // way too long
	}
	for _, in := range reject {
		if got, ok := String2ll([]byte(in)); ok {
			t.Errorf("String2ll(%q) = (%d, true), want reject", in, got)
		}
	}
}

func TestTryEncode(t *testing.T) {
	cases := []struct {
		in   string
		want Encoding
	}{
		{"123", EncInt},
		{"0", EncInt},
		{"-42", EncInt},
		{"12345", EncInt},                              // >= 10000: int but not shared
		{"9223372036854775807", EncInt},                // MaxInt64
		{"007", EncEmbstr},                             // non-canonical integer stays a string
		{"+5", EncEmbstr},                              // ditto
		{"1.5", EncEmbstr},                             // ditto
		{"hello", EncEmbstr},                           //
		{"", EncEmbstr},                                // empty string
		{"123456789012345678901", EncEmbstr},           // 21 digits: too long for int, short enough for embstr
		{"99999999999999999999", EncEmbstr},            // 20 digits but overflows int64
		{strings.Repeat("x", EmbstrCutoff), EncEmbstr}, // exactly 44
		{strings.Repeat("x", EmbstrCutoff+1), EncRaw},  // 45
		{strings.Repeat("x", 1000), EncRaw},
	}
	for _, tc := range cases {
		o := TryEncode([]byte(tc.in))
		if o.Encoding != tc.want {
			t.Errorf("TryEncode(%q).Encoding = %s, want %s",
				tc.in, o.EncodingName(), encName(tc.want))
		}
		if o.Type != TypeString {
			t.Errorf("TryEncode(%q).Type != TypeString", tc.in)
		}
		// Non-int encodings must preserve the exact bytes.
		if tc.want != EncInt && string(o.Val.([]byte)) != tc.in {
			t.Errorf("TryEncode(%q) mangled payload to %q", tc.in, o.Val)
		}
	}
}

func encName(e Encoding) string { return (&Object{Encoding: e}).EncodingName() }

func TestSharedIntegerSingletons(t *testing.T) {
	// Same value in shared range => same pointer, from both constructors.
	if NewInt(5) != NewInt(5) {
		t.Error("NewInt(5) not a singleton")
	}
	if TryEncode([]byte("5")) != NewInt(5) {
		t.Error("TryEncode(\"5\") did not return the shared singleton")
	}
	if NewInt(0) != &sharedIntegers[0] || NewInt(9999) != &sharedIntegers[9999] {
		t.Error("shared range boundaries broken")
	}

	// Outside the shared range: fresh objects.
	if NewInt(10000) == NewInt(10000) {
		t.Error("NewInt(10000) unexpectedly shared")
	}
	if NewInt(-1).IsShared() {
		t.Error("negative int reported shared")
	}

	// IsShared discriminates singleton vs equal-valued fresh object.
	if !NewInt(100).IsShared() {
		t.Error("singleton not detected as shared")
	}
	fresh := &Object{Type: TypeString, Encoding: EncInt, Val: int64(100)}
	if fresh.IsShared() {
		t.Error("fresh object with shared-range value misreported as shared")
	}
	if (&Object{Type: TypeString, Encoding: EncEmbstr, Val: []byte("100")}).IsShared() {
		t.Error("embstr misreported as shared")
	}
}

func TestNames(t *testing.T) {
	if got := TryEncode([]byte("1")).EncodingName(); got != "int" {
		t.Errorf("int encoding name = %q", got)
	}
	if got := TryEncode([]byte("hi")).EncodingName(); got != "embstr" {
		t.Errorf("embstr encoding name = %q", got)
	}
	if got := TryEncode([]byte(strings.Repeat("x", 45))).EncodingName(); got != "raw" {
		t.Errorf("raw encoding name = %q", got)
	}
	if got := TryEncode([]byte("hi")).TypeName(); got != "string" {
		t.Errorf("type name = %q", got)
	}
}
