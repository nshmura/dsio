package core

import (
	"fmt"

	"github.com/fatih/color"
)

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

func Error(message interface{}) error {
	color.Red("[ERROR] %v", message)
	fmt.Println("")
	return fmt.Errorf("%v", message)
}

func Errorf(format string, value ...interface{}) error {
	color.Red("[ERROR] "+format, value...)
	fmt.Println("")
	return fmt.Errorf(format, value...)
}

func Panicf(format string, value ...interface{}) string {
	return color.RedString("[PANIC] "+format, value...)
}
