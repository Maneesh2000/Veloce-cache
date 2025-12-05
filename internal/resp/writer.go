package resp

import "strconv"

// Reply serializers. Append-style (like Redis's addReply* family writing
// into the client output buffer): each takes the current output buffer and
// returns it with the encoded reply appended.

var crlf = []byte("\r\n")

// AppendSimpleString appends "+<s>\r\n".
func AppendSimpleString(b []byte, s string) []byte {
	b = append(b, '+')
	b = append(b, s...)
	return append(b, crlf...)
}

// AppendError appends "-<msg>\r\n".
func AppendError(b []byte, msg string) []byte {
	b = append(b, '-')
	b = append(b, msg...)
	return append(b, crlf...)
}

// AppendInt appends ":<n>\r\n".
func AppendInt(b []byte, n int64) []byte {
	b = append(b, ':')
	b = strconv.AppendInt(b, n, 10)
	return append(b, crlf...)
}

// AppendBulk appends "$<len>\r\n<payload>\r\n".
func AppendBulk(b []byte, payload []byte) []byte {
	b = append(b, '$')
	b = strconv.AppendInt(b, int64(len(payload)), 10)
	b = append(b, crlf...)
	b = append(b, payload...)
	return append(b, crlf...)
}

// AppendBulkString is AppendBulk for string payloads.
func AppendBulkString(b []byte, s string) []byte {
	b = append(b, '$')
	b = strconv.AppendInt(b, int64(len(s)), 10)
	b = append(b, crlf...)
	b = append(b, s...)
	return append(b, crlf...)
}

// AppendBulkInt64 appends v formatted as a bulk string ("$3\r\n100\r\n").
// Used to render int-encoded objects on the read path without materializing
// a byte-slice object; tmp lives on the stack.
