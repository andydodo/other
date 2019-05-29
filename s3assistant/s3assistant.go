package main

import (
	"flag"
	"fmt"
	"html/template"
	"io"
	"os"
	"os/signal"
	"s3assistant/command"
	"strings"
	"syscall"
	"time"
	"unicode"
	"unicode/utf8"
)

var (
	GIT_COMMIT  string
	BUILD_TIME  string
	APP_VERSION string
)

var commands = command.Commands

var cmdVersion = &command.Command{
	UsageLine: "version",
	Short:     "print the compile version and tag",
	Long:      "",
}

func runVersion(stop chan struct{}) bool {
	fmt.Fprintf(os.Stderr, "Version: %s\n", APP_VERSION)
	fmt.Fprintf(os.Stderr, "Git Commit: %s\n", GIT_COMMIT)
	fmt.Fprintf(os.Stderr, "Build Time: %s\n", BUILD_TIME)
	return true
}

func init() {
	cmdVersion.Run = runVersion
	commands = append(commands, cmdVersion)
}

func main() {
	flag.Usage = usage
	flag.Parse()

	args := flag.Args()
	if len(args) < 1 {
		usage()
	}

	if args[0] == "version" {
		cmdVersion.Run(nil)
		return
	}

	if args[0] == "help" {
		help(args[1:])
		for _, cmd := range commands {
			if len(args) >= 2 && cmd.Name() == args[1] && cmd.Run != nil {
				fmt.Fprintf(os.Stderr, "Default Parameters:\n")
				cmd.Flag.PrintDefaults()
			}
		}
		return
	}
	// Handle signal
	exitChan := make(chan struct{})
	go HandleSignal(exitChan)

	// Fetch command
	var excuteCmd *command.Command = nil
	for _, cmd := range commands {
		if cmd.Name() == args[0] && cmd.Run != nil {
			cmd.Flag.Usage = func() { cmd.Usage() }
			cmd.Flag.Parse(args[1:])
			excuteCmd = cmd
			break
		}
	}
	if excuteCmd == nil {
		fmt.Fprintf(os.Stderr, "unkown command: %s, useage: s3assistant help\n", args[0])
		return
	}
	// Start Cmd
	var stop chan struct{} = make(chan struct{}, 1)
	if !excuteCmd.Run(stop) {
		return
	}
	for {
		select {
		case <-exitChan:
			stop <- struct{}{}
		default:
			if excuteCmd.Status == command.CommandExited {
				return
			}
			time.Sleep(time.Second * 5)
		}
	}
	return
}

func HandleSignal(exitChan chan struct{}) {
	ch := make(chan os.Signal)
	signal.Notify(ch,
		syscall.SIGINT,
		syscall.SIGKILL,
		syscall.SIGALRM,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT,
	)
	for {
		sig := <-ch
		switch sig {
		// ignore SIGHUP and SIGQUIT
		case syscall.SIGHUP, syscall.SIGQUIT:
		default:
			exitChan <- struct{}{}
		}
	}
}

var usageTemplate = `
s3assistant: a assistant for s3-gateway!

Usage:

	s3assistant command [arguments]

The commands are:
{{range .}}{{if .Runnable}}
    {{.Name | printf "%-11s"}} {{.Short}}{{end}}{{end}}

Use "s3assistant help [command]" for more information about a command.

`

var helpTemplate = `{{if .Runnable}}Usage: s3assistant {{.UsageLine}}
{{end}}
  {{.Long}}
`

func tmpl(w io.Writer, text string, data interface{}) {
	t := template.New("top")
	t.Funcs(template.FuncMap{"trim": strings.TrimSpace, "capitalize": capitalize})
	template.Must(t.Parse(text))
	if err := t.Execute(w, data); err != nil {
		panic(err)
	}
}

func capitalize(s string) string {
	if s == "" {
		return s
	}
	r, n := utf8.DecodeRuneInString(s)
	return string(unicode.ToTitle(r)) + s[n:]
}

func printUsage(w io.Writer) {
	tmpl(w, usageTemplate, commands)
}

func usage() {
	printUsage(os.Stderr)
	os.Exit(2)
}

func help(args []string) {
	if len(args) == 0 {
		printUsage(os.Stdout)
		return
	}
	if len(args) != 1 {
		fmt.Fprintf(os.Stderr, "usage: s3assistant help command\n\nToo many arguments given.\n")
		os.Exit(2)
	}
	for _, cmd := range commands {
		if cmd.Name() == args[0] {
			tmpl(os.Stdout, helpTemplate, cmd)
			return
		}
	}
	fmt.Fprintf(os.Stderr, "Unknown help topic %#q.  Run 's3assistant help'.\n", args[0])
	os.Exit(2)
}
