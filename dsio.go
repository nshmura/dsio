package main

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/nshmura/dsio/action"
	"github.com/nshmura/dsio/core"
	"github.com/urfave/cli"
)

var (
	version = "No Version Provided"
)

const (
	// the number of entities to output at once
	defaultPageSize = 50

	// max page size
	maxPageSize = 1000
)

var (
	flagServiceAccoutFile = cli.StringFlag{
		Name:   "key-file",
		Usage:  "name of GCP service account file.",
		EnvVar: "DSIO_KEY_FILE",
	}

	flagProjectID = cli.StringFlag{
		Name:   "project-id",
		Usage:  "Project ID of GCP.",
		EnvVar: "DSIO_PROJECT_ID",
	}

	flagVerbose = cli.BoolFlag{
		Name:  "verbose, v",
		Usage: "Make the operation more talkative.",
	}

	flagNoColor = cli.BoolFlag{
		Name:  "no-color",
		Usage: "Disable color output.",
	}

	flagNamespace = cli.StringFlag{
		Name:  "namespace, n",
		Usage: "namespace of entities.",
	}
)

func main() {
	app := cli.NewApp()

	cli.VersionFlag = cli.BoolFlag{
		Name:  "version, V",
		Usage: "Show version number and quit",
	}

	app.Name = "dsio"
	app.Usage = "A command line tool for Google Cloud Datastore."
	app.Version = version

	app.Commands = []cli.Command{
		{
			Name:      "upsert",
			Usage:     "Bulk-upsert entities into Datastore.",
			ArgsUsage: "filename",
			Flags: []cli.Flag{
				flagNamespace,
				cli.StringFlag{
					Name:  "kind, k",
					Usage: "name of destination kind.",
				},
				cli.StringFlag{
					Name:  "format, f",
					Usage: "format of input file. <yaml|csv|tcv>.",
				},
				cli.BoolFlag{
					Name:  "dry-run",
					Usage: "skip Datastore operations.",
				},
				cli.IntFlag{
					Name:  "batch-size",
					Value: action.MaxBatchSize,
					Usage: fmt.Sprintf("number of entities per one multi upsert operation. batch-size should be smaller than %d.", action.MaxBatchSize),
				},
				flagServiceAccoutFile,
				flagProjectID,
				flagVerbose,
				flagNoColor,
			},
			Action: func(c *cli.Context) error {
				args := c.Args()
				if l := len(args); l == 0 {
					return core.NewExitError("Filename is not specified")

				} else if l > 1 {
					return core.NewExitError("Too many args")
				}
				filename := args[0]

				ctx := core.SetContext(c)
				ctx.PrintContext()

				err := action.Upsert(ctx, filename, c.String("kind"), c.String("format"), c.Int("batch-size"))
				if err != nil {
					return core.NewExitError(err)
				}
				return nil
			},
		},
		{
			Name:      "query",
			Usage:     "Execute a query.",
			ArgsUsage: `"[<gql_query>]"`,
			Flags: []cli.Flag{
				flagNamespace,
				cli.StringFlag{
					Name:  "output, o",
					Usage: "output filename. Entities are outputed into this file.",
				},
				cli.StringFlag{
					Name:  "format, f",
					Value: "yaml",
					Usage: "format of output. <yaml|csv|tcv>.",
				},
				cli.StringFlag{
					Name:  "style, s",
					Value: "scheme",
					Usage: "style of output. <scheme|direct|auto>. used only in yaml format.",
				},
				cli.IntFlag{
					Name:  "page-size",
					Value: defaultPageSize,
					Usage: "number of entities to output at once.",
				},
				flagServiceAccoutFile,
				flagProjectID,
				flagVerbose,
				flagNoColor,
			},
			Action: func(c *cli.Context) error {
				query := strings.Join(c.Args(), " ")

				var format = c.String("format")
				switch format {
				case core.FormatCSV, core.FormatTSV, core.FormatYAML:
				// ok
				case "":
					format = core.FormatYAML
				default:
					return core.NewExitError("Format should be yaml, csv or tsv")
				}

				style, err := getTypeStyle(c.String("style"))
				if err != nil {
					return core.NewExitError(err)
				}

				pageSize := c.Int("page-size")
				if pageSize == 0 {
					pageSize = defaultPageSize

				} else if pageSize > maxPageSize {
					return core.NewExitErrorf("too large page size:%v", pageSize)
				}

				ctx := core.SetContext(c)
				ctx.PrintContext()

				err = action.Query(ctx, query, format, style, c.String("output"), pageSize)
				if err != nil {
					return core.NewExitError(err)
				}
				return nil
			},
		},
	}

	app.Run(os.Args)
}

func getTypeStyle(style string) (core.TypeStyle, error) {
	switch style {
	case string(core.StyleScheme), string(core.StyleDirect), string(core.StyleAuto):
		return core.TypeStyle(style), nil
	case "":
		return core.StyleScheme, nil
	default:
		return core.TypeStyle(""), errors.New("Format should be one of scheme, direct, auto")
	}
}
