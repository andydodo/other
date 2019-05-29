package util

import (
	"fmt"
)

type HandlerError struct {
	Handler   string
	ErrorInfo string
}

func (h *HandlerError) Error() string {
	return fmt.Sprintf("HandlerError occurred, handler: %s, ErrorInfo: %s", h.Handler, h.ErrorInfo)
}

type ParseError struct {
	Message string
}

func (p *ParseError) Error() string {
	return fmt.Sprintf("ParseError occurred, message: %s", p.Message)
}

type HttpError struct {
	Function, URI, Message string
	HttpCode               int
}

func (h *HttpError) Error() string {
	return fmt.Sprintf("HttpError occurred, Function: %s, HttpCode: %d, URI: %s, Message: %s",
		h.Function, h.HttpCode, h.URI, h.Message)
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

type S3Error struct {
	Bucket   string
	Key      string
	Function string
}

func (s *S3Error) Error() string {
	return fmt.Sprintf("S3Error occurred, Function: %s, Bucket: %s, Key: %s",
		s.Function, s.Bucket, s.Key)
}
