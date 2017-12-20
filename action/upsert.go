package action

import (
	"context"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"strings"

	"cloud.google.com/go/datastore"
	"github.com/nshmura/dsio/core"
)

const (
	// MaxBatchSize The number of entities per one multi upsert operation
	MaxBatchSize = 500
)

// Upsert entities form yaml file to datastore
func Upsert(ctx core.Context, filename, kind, format string, batchSize int) error {

	// Format
	switch format {
	case core.FormatCSV, core.FormatTSV, core.FormatYAML:
		// ok
	case "":
		var err error
		if format, err = detectFileFormat(filename); err != nil {
			return errors.New("can not detect file format")
		}
	default:
		return fmt.Errorf("format should be yaml, csv or tsv. :%s", format)
	}

	// BatchSize
	if batchSize == 0 {
		batchSize = MaxBatchSize
	} else if batchSize > MaxBatchSize {
		return fmt.Errorf("batch-size should be smaller than %d\n", MaxBatchSize)
	}

	// Parser
	parser := getParser(format)

	// Read from file
	if err := parser.ReadFile(filename); err != nil {
		return err
	}

	// Parse
	dsEntities, err := parser.Parse(kind)
	if err != nil {
		return err
	}

	// Upsert to datastore
	if !ctx.DryRun {
		client, err := core.CreateDatastoreClient(ctx)
		if err != nil {
			return err
		}

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
					return err
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
							return fmt.Errorf("Upsert error(entity No.%v): %v\n", i+1, e)
						}
					}
				} else {
					return fmt.Errorf("Upsert error: %v\n", err)
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

func getParser(format string) core.FileParser {
	switch format {
	case core.FormatCSV:
		return core.NewCSVParser(',')
	case core.FormatTSV:
		return core.NewCSVParser('\t')
	default:
		return core.NewYAMLParser()
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
