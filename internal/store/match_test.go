package store

import (
	"strings"
	"testing"
	"time"
)

func TestMatch(t *testing.T) {
	cases := []struct {
		pattern, s string
		want       bool
	}{
		// Literals.
		{"hello", "hello", true},
		{"hello", "hellx", false},
		{"hello", "hell", false},
		{"", "", true},
		{"", "x", false},

		// Star.
		// NOTE: modern stringmatchlen requires a non-empty string to enter
		// its loop, so "*" does NOT match "" — faithfully ported quirk.
		// KEYS is unaffected: the literal pattern "*" is shortcut before
		// the matcher runs (both in Redis and in DB.Keys).
		{"*", "", false},
		{"*", "anything", true},
		{"h*llo", "hllo", true},
		{"h*llo", "heeeello", true},
		{"h*llo", "hllx", false},
		{"h*", "hello", true},
		{"*llo", "hello", true},
		{"a*b*c", "aXXbYYc", true},
		{"a*b*c", "aXXbYY", false},
		{"**", "x", true}, // consecutive stars collapse

		// Question mark.
		{"h?llo", "hello", true},
		{"h?llo", "hallo", true},
		{"h?llo", "hllo", false},
		{"h?llo", "heello", false},
		{"?", "", false},

		// Classes.
		{"h[ae]llo", "hello", true},
		{"h[ae]llo", "hallo", true},
		{"h[ae]llo", "hillo", false},
		{"h[^e]llo", "hallo", true},
		{"h[^e]llo", "hello", false},
		{"h[a-b]llo", "hallo", true},
		{"h[a-b]llo", "hbllo", true},
		{"h[a-b]llo", "hcllo", false},
		{"h[b-a]llo", "hallo", true}, // reversed range auto-swaps
		{"h[b-a]llo", "hbllo", true},
		{"[\\]]x", "]x", true}, // escaped ']' inside class
		{"h[ae", "ha", true},   // unterminated class tolerated
		{"h[ae", "hx", false},
		{"[a]", "", false}, // class needs a byte to consume

		// Escapes.
		{"\\*", "*", true},
		{"\\*", "x", false},
		{"\\?", "?", true},
		{"a\\", "a\\", true}, // trailing lone backslash = literal backslash
		{"\\\\", "\\", true},

		// KEYS-style compound patterns.
		{"user:*", "user:1000", true},
		{"user:*", "session:1", false},
		{"user:?00", "user:100", true},
		{"user:?00", "user:1000", false},
		{"*:100*", "user:1000", true},

		// Binary safety.
		{"a?c", "a\x00c", true},
		{"a*c", "a\x00\xffc", true},
	}
	for _, tc := range cases {
		if got := Match([]byte(tc.pattern), []byte(tc.s)); got != tc.want {
			t.Errorf("Match(%q, %q) = %v, want %v", tc.pattern, tc.s, got, tc.want)
		}
	}
}

// TestMatchAdversarialPattern: the classic glob blowup a*a*a*...b against
// aaaa...a must terminate quickly via the nesting guard, not hang the event
// loop for minutes.
func TestMatchAdversarialPattern(t *testing.T) {
	pattern := []byte(strings.Repeat("a*", 30) + "b")
	subject := []byte(strings.Repeat("a", 200))
	done := make(chan bool, 1)
	go func() { done <- Match(pattern, subject) }()
	select {
	case got := <-done:
		if got {
			t.Error("adversarial pattern unexpectedly matched")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Match did not terminate within 5s on adversarial pattern")
	}
}
