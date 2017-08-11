package main

import (
	"errors"
	"os"
	"strings"

	"github.com/nshmura/dsio/action"
	"github.com/nshmura/dsio/core"
	"github.com/urfave/cli"
)

const (
	// the number of entities to output at once
	defaultPageSize = 50

	// max page size
	maxPageSize = 1000
)

var (
	FlagServiceAccoutFile = cli.StringFlag{
		Name:   "key-file",
		Usage:  "JSON `KEYFILE` of GCP service account",
		EnvVar: "DSIO_KEY_FILE",
	}

	FlagProjectID = cli.StringFlag{
		Name:   "project-id",
		Usage:  "GCP `PROJECT_ID`",
		EnvVar: "DSIO_PROJECT_ID",
	}

	FlagVerbose = cli.BoolFlag{
		Name:  "verbose, v",
		Usage: "Make the operation more talkative",
	}

	FlagNoColor = cli.BoolFlag{
		Name:  "no-color",
		Usage: "Disable color output",
	}

	FlagNamespace = cli.StringFlag{
		Name:  "namespace, n",
		Usage: "`NAMESPACE` of entities",
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
	app.Version = "0.1.0"

	app.Commands = []cli.Command{
		{
			Name:      "query",
			Usage:     "Query entities by GQL from Datastore",
			ArgsUsage: `"SELECT * FROM ..."`,
			Flags: []cli.Flag{
				FlagNamespace,
				cli.StringFlag{
					Name:  "output, o",
					Usage: "Write entities to `FILE`",
				},
				cli.StringFlag{
					Name:  "format, f",
					Usage: "Query entities as `FORMAT` (yaml, csv, tcv)",
				},
				cli.StringFlag{
					Name:  "style, s",
					Usage: "Propertie's types are specified by `TYPE` style (scheme, direct, auto)",
				},
				cli.IntFlag{
					Name:  "page-size",
					Usage: "The `NUMBER` of entities to output at once",
				},
				FlagServiceAccoutFile,
				FlagProjectID,
				FlagVerbose,
				FlagNoColor,
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
					core.Errorf("Format should be one of yaml, csv, tsv")
					return nil
				}

				style, err := getTypeStyle(c.String("style"))
				if err != nil {
					core.Error(err)
					return nil
				}

				pageSize := c.Int("page-size")
				if pageSize == 0 {
					pageSize = defaultPageSize

				} else if pageSize > maxPageSize {
					return core.Errorf("Too large page size:%v", pageSize)
				}

				ctx := core.SetContext(c)
				ctx.PrintContext()
				action.Query(ctx, query, format, style, c.String("output"), pageSize)
				return nil
			},
		},
		{
			Name:  "upsert",
			Usage: "Bulk upsert entities to Datastore",
			Flags: []cli.Flag{
				FlagNamespace,
				cli.StringFlag{
					Name:  "input, i",
					Usage: "Read entities from `FILE` (required)",
				},
				cli.BoolFlag{
					Name:  "dry-run",
					Usage: "Skip operations of datastore",
				},
				cli.IntFlag{
					Name:  "batch-size",
					Usage: "The number of entities per one multi upsert operation. batch-size should be smaller than 500",
				},
				FlagServiceAccoutFile,
				FlagProjectID,
				FlagVerbose,
				FlagNoColor,
			},
			Action: func(c *cli.Context) error {

				if c.String("input") != "" {
					ctx := core.SetContext(c)
					ctx.PrintContext()
					action.UpsertFromYAML(ctx, c.String("input"), c.Int("batch-size"))

				} else {
					core.Error("Please set `--input` option")
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
