package core

import (
	"github.com/fatih/color"
	"github.com/urfave/cli"
)

var (
	ctx Context
)

type Context struct {
	ServiceAccountFile string
	ProjectID          string
	Namespace          string
	NoColor            bool
	DryRun             bool
	Verbose            bool
}

func SetContext(c *cli.Context) Context {

	color.NoColor = c.GlobalBool("no-color")

	ctx = Context{
		ServiceAccountFile: c.String("key-file"),
		ProjectID:          c.String("project-id"),
		Verbose:            c.Bool("verbose"),
		NoColor:            c.Bool("no-color"),
		Namespace:          c.String("namespace"),
		DryRun:             c.Bool("dry-run"),
	}
	return ctx
}

func (ctx Context) PrintContext() {
	if ctx.Verbose {
		Debug("")
		Debugf("service-account-file: %v\n", ctx.ServiceAccountFile)
		Debugf("project-id: %v\n", ctx.ProjectID)
		Debugf("namespace: %v\n", ctx.Namespace)
		Debugf("dry-run: %v\n", ctx.DryRun)
		Debug("")
	}
}
