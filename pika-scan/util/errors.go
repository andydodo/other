package util

import "fmt"

type ParseError struct {
	Message string
}

func (p *ParseError) Error() string {
	return fmt.Sprintf("ParseError occurred, message: %s", p.Message)
}

type RedisError struct {
	Message   string
	Function  string
	ErrorInfo string
}

func (r *RedisError) Error() string {
	return fmt.Sprintf("RedisError occurred, Function: %s, Message: %s, ErrorInfo: %s",
		r.Function, r.Message, r.ErrorInfo)
}
