package gql

func isWhitespace(ch rune) bool {
	return ch == ' ' || ch == '\t' || ch == '\n'
}

func isLetter(ch rune) bool {
	return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') ||
		(ch >= '\u0080' && ch <= '\uFFFF') || ch == '_' || ch == '$'
}

func isDigit(ch rune) bool {
	return ch >= '1' && ch <= '9'
}

func isNumber(ch rune) bool {
	return ch >= '0' && ch <= '9'
}

func isQuote(ch rune) bool {
	return ch == '\'' || ch == '"'
}

func isBackquote(ch rune) bool {
	return ch == '`'
}

var newline = rune('\n')
var eof = rune(0)
