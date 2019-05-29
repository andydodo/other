package command

import (
	"flag"
	"fmt"
	"os"
	"s3assistant/common"
	"strings"
)

const DefaultSleepTime = 5

var Commands = []*Command{
	cmdAsyncDelete,
	cmdMigration,
	cmdBackup,
}

type CommandOptions interface {
	CreateCommandEntry() common.HandlerEntry
}

type CommandStatus int

const (
	CommandInit CommandStatus = iota
	CommandProcessing
	CommandExited
)

type Command struct {
	// Run runs the command.
	// stop is a channel to handle signal.
	Run func(stop chan struct{}) bool

	// UsageLine is the one-line usage message.
	// The first word in the line is taken to be the command name.
	UsageLine string

	// Short is the short description shown in the 'go help' output.
	Short string

	// Long is the long message shown in the 'go help <this-command>' output.
	Long string

	// Flag is a set of flags specific to this command.
	Flag flag.FlagSet

	// Status is the process state to this command
	Status CommandStatus
}

func (c *Command) Name() string {
	name := c.UsageLine
	i := strings.Index(name, " ")
	if i >= 0 {
		name = name[:i]
	}
	return name
}

func (c *Command) Usage() {
	fmt.Fprintf(os.Stderr, "Example: s3assistant %s\n", c.UsageLine)
	fmt.Fprintf(os.Stderr, "Default Usage:\n")
	c.Flag.PrintDefaults()
	fmt.Fprintf(os.Stderr, "Description:\n")
	fmt.Fprintf(os.Stderr, "  %s\n", strings.TrimSpace(c.Long))
	os.Exit(2)
}

func (c *Command) Runnable() bool {
	return c.Run != nil
}
