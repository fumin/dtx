package dynamodb

import (
	"strings"
)

type UpdateExpression struct {
	Set    []string
	Remove []string
	Add    []string
	Delete []string
}

func startsWithKeyword(runes []rune) []rune {
	upper := strings.ToUpper(string(runes))
	for _, keyword := range []string{"SET", "REMOVE", "ADD", "DELETE"} {
		kw := " " + keyword + " "
		if strings.HasPrefix(upper, kw) {
			return runes[:len(kw)]
		}
	}
	return []rune{}
}

func setKeyword(expr *UpdateExpression, keyword []rune, body []rune) {
	splitted := strings.Split(string(body), ",")
	trimmed := make([]string, 0, len(splitted))
	for _, s := range splitted {
		trimmed = append(trimmed, strings.TrimSpace(s))
	}

	if len(keyword)-1 < 1 {
		return
	}
	trimmedKW := keyword[1 : len(keyword)-1] // remove white spaces
	kw := strings.ToUpper(string(trimmedKW))
	switch kw {
	case "SET":
		expr.Set = trimmed
	case "REMOVE":
		expr.Remove = trimmed
	case "ADD":
		expr.Add = trimmed
	case "DELETE":
		expr.Delete = trimmed
	}
}

func ParseUpdateExpr(s *string) *UpdateExpression {
	if s == nil {
		return &UpdateExpression{}
	}

	runes := []rune(" " + *s)

	expr := &UpdateExpression{}
	keyword := []rune{}
	body := []rune{}
	i := 0
	for i < len(runes) {
		newkw := startsWithKeyword(runes[i:])
		if len(newkw) > 0 {
			setKeyword(expr, keyword, body)
			keyword = newkw
			body = []rune{}

			i += len(keyword)
			continue
		}

		body = append(body, runes[i])
		i++
	}
	setKeyword(expr, keyword, body)

	return expr
}

func (expr *UpdateExpression) String() string {
	sections := make([]string, 0, 4)
	if len(expr.Set) > 0 {
		sections = append(sections, "SET "+strings.Join(expr.Set, ", "))
	}
	if len(expr.Remove) > 0 {
		sections = append(sections, "REMOVE "+strings.Join(expr.Remove, ", "))
	}
	if len(expr.Add) > 0 {
		sections = append(sections, "ADD "+strings.Join(expr.Add, ", "))
	}
	if len(expr.Delete) > 0 {
		sections = append(sections, "DELETE "+strings.Join(expr.Delete, ", "))
	}

	res := strings.Join(sections, " ")
	return res
}
