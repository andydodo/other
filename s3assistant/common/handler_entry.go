package common

type HandlerStatus int

const (
	HANDLER_NONE HandlerStatus = iota
	HANDLER_INITIAL
	HANDLER_PROCESSING
	HANDLER_EXITED
)

// Used for all s3assistant command
type HandlerEntry interface {
	Init() bool
	Start()
	Status() HandlerStatus
	Exit()
}
