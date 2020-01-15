package action

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"cloud.google.com/go/datastore"
	"github.com/nshmura/dsio/core"
	"github.com/nshmura/dsio/gql"
	"google.golang.org/api/iterator"
)

// Query entities from datastore to stdout
func Query(ctx core.Context, gqlStr, format string, style core.TypeStyle, filename string, pageSize int) error {

	// Prepare io.writer
	var writer io.Writer = os.Stdout
	if filename != "" {
		fp, err := openFile(filename)
		if fp == nil {
			return nil
		}
		defer fp.Close()
		if err != nil {
			return err
		}
		w := bufio.NewWriter(fp)
		defer w.Flush()
		writer = w
	}

	if gqlStr == "" {
		var err error
		gqlStr, err = readQuery()
		if err != nil {
			return err
		}
	}

	kind, q, err := getKindQuery(ctx, gqlStr)
	if err != nil {
		if err == io.EOF {
			return nil
		}
		return err
	}

	core.Debugf("kind = %v\n", kind)
	core.Debugf("query = %q\n", q)

	// Exporter
	exporter := getExporter(ctx, format, style, kind, writer)

	// Output entities
	if err = outputEntities(ctx, pageSize, kind, q, exporter); err != nil {
		return err
	}
	return nil

}

func readQuery() (string, error) {
	info, err := os.Stdin.Stat()
	if err != nil {
		return "", err
	}

	if (info.Mode() & os.ModeCharDevice) == os.ModeCharDevice {
		fmt.Print("gql> ")
		return bufio.NewReader(os.Stdin).ReadString('\n')

	} else {
		return "", errors.New("pipe not supported")
	}
}

func getKindQuery(ctx core.Context, gqlStr string) (string, *datastore.Query, error) {

	// Parse GQL
	selectExpr, err := parseGQL(gqlStr)
	if err != nil {
		return "", nil, err
	}

	// Convert to datastore's query
	kind, q, err := convertToDatastoreQuery(ctx.Namespace, selectExpr)
	if err != nil {
		return "", nil, err
	}

	return kind, q, nil
}

func parseGQL(gqlStr string) (*gql.SelectExpr, error) {
	l := new(gql.Lexer)
	l.Scanner = gql.NewScanner(strings.NewReader(gqlStr))
	if err := l.Parse(); err != nil {
		return nil, errors.New(strings.Trim(err.Error(), "\n"))
	}
	selectExpr, ok := l.Result.(gql.SelectExpr)
	if !ok {
		return nil, fmt.Errorf("can not convert to Select: %v", l.Result)
	}
	return &selectExpr, nil
}

func convertToDatastoreQuery(namespace string, s *gql.SelectExpr) (string, *datastore.Query, error) {

	// Kind
	var kind string
	if s.From != nil && s.From.Kind != nil {
		kind = s.From.Kind.Name
	} else {
		return "", nil, errors.New("sorry. kindless query is not supported") //TODO
	}

	q := datastore.NewQuery(kind).Namespace(namespace)

	// Fields
	if s.Field.Distinct {
		q = q.Distinct()
	}
	flen := len(s.Field.Field)
	if flen == 1 {
		if core.IsKeyValueName(s.Field.Field[0]) {
			q = q.KeysOnly()
		} else {

			q = q.Project(s.Field.Field[0])
		}
	} else if flen > 1 {
		q = q.Project(s.Field.Field...)
	}
	if len(s.Field.DistinctOnField) > 0 {
		q = q.DistinctOn(s.Field.DistinctOnField...)
	}

	// Filter
	q, err := setFilter(q, s.Where, namespace)
	if err != nil {
		return "", nil, err
	}

	// Order
	if len(s.Order) > 0 {
		for _, o := range s.Order {
			sort := ""
			if o.Sort == gql.DESC {
				sort = "-"
			}
			q = q.Order(sort + o.PropertyName)
		}
	}

	// Limit
	if s.Limit != nil {
		if s.Limit.Cursor != "" {
			return "", nil, fmt.Errorf("cursor is not supported: %v", s.Limit.Cursor)
		}
		q = q.Limit(s.Limit.Number)
	}

	// Offset
	if s.Offset != nil {
		if s.Offset.Cursor != "" {
			return "", nil, fmt.Errorf("cursor not supported: %v", s.Offset.Cursor)
		}
		q = q.Offset(s.Offset.Number)
	}

	return kind, q, nil
}

func setFilter(q *datastore.Query, where []gql.ConditionExpr, namespace string) (*datastore.Query, error) {

	for _, c := range where {
		switch c.GetComparator() {
		case gql.OP_IS_NULL:
			return nil, errors.New("sorry. 'IS NULL' is not supported") //TODO

		case gql.OP_CONTAINS:
			return nil, errors.New("sorry. 'CONTAINS' is not supported") //TODO

		case gql.OP_IN:
			return nil, errors.New("sorry. 'IN' is not supported") //TODO

		case gql.OP_HAS_DESCENDANT:
			return nil, errors.New("sorry. 'HAS DESCENDANT' is not supported") //TODO

		case gql.OP_HAS_ANCESTOR:
			if v, ok := c.GetValue().V.(gql.KeyLiteralExpr); ok {
				var key *datastore.Key
				for _, k := range v.KeyPath {
					if k.Name != "" {
						key = datastore.NameKey(k.Kind, k.Name, key)
					} else if k.ID > 0 {
						key = datastore.NameKey(k.Kind, k.Name, key)
					} else {
						return nil, fmt.Errorf("invalid ANCESTOR key %v", k)
					}
				}
				q = q.Ancestor(key)
			} else {
				return nil, fmt.Errorf("invalid ANCESTOR value %v", c.GetValue().V)
			}

		case gql.OP_EQUALS:
			v := c.GetValue().V
			if key, ok := v.(gql.KeyLiteralExpr); ok {
				v = key.ToDatastoreKey(namespace)
			}
			q = q.Filter(fmt.Sprintf("%s =", c.GetPropertyName()), v)

		case gql.OP_LESS:
			q = q.Filter(fmt.Sprintf("%s <", c.GetPropertyName()), c.GetValue().V)

		case gql.OP_LESS_EQUALS:
			q = q.Filter(fmt.Sprintf("%s <=", c.GetPropertyName()), c.GetValue().V)

		case gql.OP_GREATER:
			q = q.Filter(fmt.Sprintf("%s >", c.GetPropertyName()), c.GetValue().V)

		case gql.OP_GREATER_EQUALS:
			q = q.Filter(fmt.Sprintf("%s >=", c.GetPropertyName()), c.GetValue().V)
		}
	}

	return q, nil
}

func openFile(fn string) (*os.File, error) {

	if _, err := os.Stat(fn); !os.IsNotExist(err) {
		ok, err := core.ConfirmYesNo("File exists. Do you want to over-write?")
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, nil
		}
	}

	fp, err := os.Create(fn)
	if err != nil {
		return nil, err
	}

	return fp, nil
}

func getExporter(ctx core.Context, format string, style core.TypeStyle, kind string, writer io.Writer) core.Exporter {

	// Exporter
	switch format {
	case core.FormatCSV:
		return core.NewCSVExporter(writer, ',')
	case core.FormatTSV:
		return core.NewCSVExporter(writer, '\t')
	default:
		return core.NewYAMLExport(writer, style, ctx.Namespace, kind)
	}
}

func outputEntities(ctx core.Context, pageSize int, kind string, q *datastore.Query, exporter core.Exporter) error {

	// Get Iterator
	client, err := core.CreateDatastoreClient(ctx)
	if err != nil {
		return err
	}

	iter := client.Run(context.Background(), q)

	first := true
	keys := make([]*datastore.Key, 0)
	entities := make([]datastore.PropertyList, 0)
	from := 1
	to := 1
	for {
		var entity datastore.PropertyList
		key, err := iter.Next(&entity)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return err
		}

		keys = append(keys, key)
		entities = append(entities, entity)
		to++

		if len(entities) >= pageSize {
			if first {
				first = false
				if err := exporter.DumpScheme(keys, entities); err != nil {
					return err
				}
			}
			if err = exporter.DumpEntities(keys, entities); err != nil {
				return err
			}
			core.Infof("%d entities ware successfully outputed. (No.%d - No.%d)\n", to-from, from, to-1)
			from = to

			keys = make([]*datastore.Key, 0)
			entities = make([]datastore.PropertyList, 0)

			ok, err := core.ConfirmYesNoWithDefault("Do you want to output more entities?", true)
			if err != nil {
				return err
			}
			if !ok {
				return nil
			}
		}
	}

	if len(entities) > 0 {
		if first {
			if err := exporter.DumpScheme(keys, entities); err != nil {
				return err
			}
		}
		if err := exporter.DumpEntities(keys, entities); err != nil {
			return err
		}
		core.Infof("%d entities ware successfully outputed. (No.%d - No.%d)\n", to-from, from, to-1)
	}

	return nil
}
