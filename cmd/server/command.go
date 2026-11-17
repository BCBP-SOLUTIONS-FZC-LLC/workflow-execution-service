package main

import "fmt"

type command int

const (
	serveCommand command = iota
	migrateCommand
)

const usageText = `usage: execution-server [migrate | version]
  (no argument)  run the API server
  migrate        apply the database migrations and exit
  version        print the build version and exit`

type usageError struct{ arg string }

func (e *usageError) Error() string {
	return fmt.Sprintf("unknown argument %q\n%s", e.arg, usageText)
}

func parseCommand(args []string) (command, error) {
	if len(args) == 0 {
		return serveCommand, nil
	}
	if args[0] == "migrate" && len(args) == 1 {
		return migrateCommand, nil
	}
	return serveCommand, &usageError{arg: args[0]}
}
