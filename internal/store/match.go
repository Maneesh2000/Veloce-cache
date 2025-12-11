package store

// Match is an exact port of stringmatchlen (util.c): the glob dialect used
// by KEYS (and later SCAN MATCH, PSUBSCRIBE...). Supported syntax:
//
//   - any sequence (including empty)
//     ?        any single byte
//     [abc]    byte class; [^abc] negated; [a-z] range (reversed ranges swap)
//     \x       literal x (a trailing lone backslash matches a literal '\')
//
// Byte-oriented and binary-safe. Two faithfully-ported subtleties:
//
//   - skipLongerMatches: when the pattern after a '*' fails to match at
//     every position of the remaining string, no enclosing '*' can succeed
//     by consuming more characters either — the shared flag aborts the whole
//     backtracking cascade. This is what defuses adversarial patterns like
//     "a*a*a*...b"; the nesting cap (1000) only bounds depth.
//   - The main loop requires BOTH pattern and string to be non-empty, so
//     Match("*", "") is false in modern Redis (KEYS is unaffected — it
//     shortcuts the exact pattern "*" without calling the matcher, and so
//     does DB.Keys).
func Match(pattern, s []byte) bool {
	var skipLongerMatches bool
	return matchImpl(pattern, s, &skipLongerMatches, 0)
}

func matchImpl(pattern, s []byte, skipLongerMatches *bool, nesting int) bool {
	// Protection against abusive patterns.
	if nesting > 1000 {
		return false
	}
	for len(pattern) > 0 && len(s) > 0 {
		switch pattern[0] {
		case '*':
			// Collapse consecutive stars.
			for len(pattern) >= 2 && pattern[1] == '*' {
				pattern = pattern[1:]
			}
			if len(pattern) == 1 {
				return true // trailing * matches everything left
			}
			// Try the rest of the pattern at every start in the string.
			for len(s) > 0 {
				if matchImpl(pattern[1:], s, skipLongerMatches, nesting+1) {
					return true
				}
				if *skipLongerMatches {
					return false
				}
				s = s[1:]
			}
			// The rest of the pattern matched nowhere in the rest of the
			// string, so an earlier '*' trying a longer substring cannot
			// succeed either: it would need this same rest-of-pattern to
			// match starting even later.
			*skipLongerMatches = true
			return false

		case '?':
			s = s[1:]
			pattern = pattern[1:]

		case '[':
			pattern = pattern[1:] // skip '['
			not := len(pattern) > 0 && pattern[0] == '^'
			if not {
				pattern = pattern[1:]
			}
			matched := false
		class:
			for {
				switch {
				case len(pattern) >= 2 && pattern[0] == '\\':
					pattern = pattern[1:]
					if pattern[0] == s[0] {
						matched = true
					}
					pattern = pattern[1:]
				case len(pattern) == 0:
					break class // unterminated class tolerated, like C
				case pattern[0] == ']':
					pattern = pattern[1:]
					break class
				case len(pattern) >= 3 && pattern[1] == '-':
					start, end := pattern[0], pattern[2]
					if start > end {
						start, end = end, start
					}
					if s[0] >= start && s[0] <= end {
						matched = true
					}
					pattern = pattern[3:]
				default:
					if pattern[0] == s[0] {
						matched = true
					}
					pattern = pattern[1:]
				}
			}
			if not {
				matched = !matched
			}
			if !matched {
				return false
			}
			s = s[1:]

		default:
			// '\\' escape: match the next pattern byte literally. A trailing
			// lone backslash falls through and matches a literal '\'.
			if pattern[0] == '\\' && len(pattern) >= 2 {
				pattern = pattern[1:]
			}
			if pattern[0] != s[0] {
				return false
			}
			s = s[1:]
			pattern = pattern[1:]
		}

		// String exhausted: only trailing stars in the pattern can still
		// match (they consume nothing).
		if len(s) == 0 {
			for len(pattern) > 0 && pattern[0] == '*' {
				pattern = pattern[1:]
			}
			break
		}
	}
	return len(pattern) == 0 && len(s) == 0
}
