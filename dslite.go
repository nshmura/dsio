package main

import (
	"os"

	"github.com/urfave/cli"
	"github.com/nshmura/dslite/action"
)

func main() {
	app := cli.NewApp()

	app.Commands = []cli.Command{
		{
			Name:        "upsert",
			Usage:       "upsert entities",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name: "filename, f",
					Usage: "Filename to use to create entities (required).",
				},
			},
			Action: func(c *cli.Context) error {
				if c.IsSet("filename") {
					action.Upsert(c.String("filename"))
				} else {
					cli.ShowCommandHelp(c, c.Command.Name)
				}
				return nil
			},
		},
	}

	app.Run(os.Args)
}
