package core

import (
	"errors"
	"fmt"

	"github.com/fatih/color"
	"github.com/urfave/cli"
)

func Conform(message interface{}) {
	fmt.Printf("%v %v", color.CyanString("[CONFIRM]"), message)
}

func Conformf(format string, value ...interface{}) {
	fmt.Printf("%v ", color.CyanString("[CONFIRM]"))
	fmt.Printf(format, value...)
}
func Info(message interface{}) {
	fmt.Printf("%v %v\n", color.GreenString("[INFO]"), message)
}

func Infof(format string, value ...interface{}) {
	fmt.Printf("%v ", color.GreenString("[INFO]"))
	fmt.Printf(format, value...)
}

func Debug(message interface{}) {
	if ctx.Verbose {
		fmt.Printf("%v %v\n", color.CyanString("[DEBUG]"), message)
	}
}

func Debugf(format string, value ...interface{}) {
	if ctx.Verbose {
		fmt.Printf("%v ", color.CyanString("[DEBUG]"))
		fmt.Printf(format, value...)
	}
}

func NewExitError(message interface{}) *cli.ExitError {
	return cli.NewExitError(errors.New(color.RedString("[ERROR] %v", message)), 1)
}

func NewExitErrorf(format string, value ...interface{}) error {
	return cli.NewExitError(color.RedString("[ERROR] "+format, value...), 1)
}
