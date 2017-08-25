package action

import (
	"context"
	"fmt"
	"math"

	"path/filepath"
	"strings"

	"cloud.google.com/go/datastore"
	"github.com/nshmura/dsio/core"
)

const (
	// The number of entities per one multi upsert operation
	maxBatchSize = 500
)

// Upsert entities form yaml file to datastore
func Upsert(ctx core.Context, filename, kind, format string, batchSize int) error {

	if !ctx.Verbose {
		defer func() {
			if r := recover(); r != nil {
				fmt.Printf("%v\n\n", r)
			}
		}()
	}

	// Format
	switch format {
	case core.FormatCSV, core.FormatTSV, core.FormatYAML:
		// ok
	case "":
		var err error
		if format, err = detectFileFormat(filename); err != nil {
			core.Error("can not detect file format.")
			return nil
		}
	default:
		core.Errorf("Format should be yaml, csv or tsv. :%s", format)
		return nil
	}

	// BatchSize
	if batchSize == 0 {
		batchSize = maxBatchSize
	} else if batchSize > maxBatchSize {
		return core.Errorf("batch-size should be smaller than %d\n", maxBatchSize)
	}

	// Parser
	parser, err := getParser(kind, format)
	if err != nil {
		return core.Error(err)
	}

	// Read from file
	if err := parser.ReadFile(filename); err != nil {
		return core.Error(err)
	}

	// Parse
	dsEntities, err := parser.Parse()
	if err != nil {
		return core.Error(err)
	}

	// Upsert to datastore
	if !ctx.DryRun {
		client := core.CreateDatastoreClient(ctx)

		allPage := int(math.Ceil(float64(len(*dsEntities)) / float64(batchSize)))
		for page := 0; page < allPage; page++ {

			from := page * batchSize
			to := (page + 1) * batchSize
			if to > len(*dsEntities) {
				to = len(*dsEntities)
			}

			// Confirm
			if page > 0 {
				msg := fmt.Sprintf("Do you want to upsert more entities (No.%d - No.%d)? ", from+1, to)
				ok, err := core.ConfirmYesNoWithDefault(msg, true)
				if err != nil {
					core.Error(err)
					break
				}
				if !ok {
					break
				}
			}

			core.Infof("Upserting %d entities...\n", to-from)

			// Upsert multi entities
			keys, src := getKeysValues(ctx, dsEntities, from, to)

			if _, err := client.PutMulti(context.Background(), keys, src); err != nil {
				if me, ok := err.(datastore.MultiError); ok {
					for i, e := range me {
						if e != nil {
							core.Errorf("Upsert error(entity No.%v): %v\n", i+1, e)
						}
					}
				} else {
					core.Errorf("Upsert error: %v\n", err)
				}
			} else {
				core.Infof("%d entities ware upserted successfully.\n", len(keys))
			}
		}
	}
	return nil
}

func detectFileFormat(filename string) (string, error) {
	ext := strings.ToLower(filepath.Ext(filename))
	if ext == "" || strings.HasSuffix(ext, ".") {
		return "", nil
	}
	ext = ext[1:]

	switch ext {
	case core.FormatCSV, core.FormatTSV, core.FormatYAML:
		return ext, nil
	default:
		return "", fmt.Errorf("unknown file extension: %s", ext)
	}
}

func getParser(kind, format string) (core.FileParser, error) {
	switch format {
	case core.FormatCSV:
		return core.NewCSVParser(kind, ',')
	case core.FormatTSV:
		return core.NewCSVParser(kind, '\t')
	default:
		return core.NewYAMLParser(kind)
	}
}

func getKeysValues(ctx core.Context, dsEntities *[]datastore.Entity, from, to int) (keys []*datastore.Key, values []interface{}) {

	// Prepare entities
	for _, e := range (*dsEntities)[from:to] {

		k := core.KeyToString(e.Key)
		if k == `""` {
			k = "(auto)"
		}
		if ctx.Verbose {
			core.Infof(" entity> Key=%v Props=%v\n", k, e.Properties)
		} else {
			core.Infof(" entity> Key=%v\n", k)
		}

		keys = append(keys, e.Key)
		props := datastore.PropertyList(e.Properties)
		values = append(values, &props)
	}

	return
}
