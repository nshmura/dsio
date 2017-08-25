package core

import (
	"encoding/base64"
	"encoding/csv"
	"fmt"
	"io"
	"reflect"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/datastore"
)

type CSVExporter struct {
	writer    io.Writer
	separator rune
	namespace string
	kind      string
}

func NewCSVExporter(writer io.Writer, separator rune, namespace, kind string) *CSVExporter {
	return &CSVExporter{
		writer:    writer,
		separator: separator,
		namespace: namespace,
		kind:      kind,
	}
}

func (exp *CSVExporter) DumpScheme(keys []*datastore.Key, properties []datastore.PropertyList) error {
	propInfos := getPropInfos(properties)

	scheme, err := exp.getScheme(propInfos)
	if err != nil {
		return err
	}
	exp.outputYaml("scheme", scheme)

	return nil
}

func (exp *CSVExporter) DumpEntities(keys []*datastore.Key, properties []datastore.PropertyList) error {

	writer := csv.NewWriter(exp.writer)
	writer.Comma = exp.separator

	propInfos := getPropInfos(properties)

	headers := []string{KeywordKey}

	for _, info := range propInfos {
		headers = append(headers, info.Name)
	}
	writer.Write(headers)

	for i, e := range properties {
		props, err := e.Save()

		if err != nil {
			return err

		} else {
			values, err := exp.getValueList(propInfos, props)
			if err != nil {
				return err
			}
			values = append([]string{KeyToString(keys[i])}, values...)
			writer.Write(values)
		}
	}

	writer.Flush()
	return nil
}

func (exp *CSVExporter) getValueList(propInfos []PropertyInfo, props []datastore.Property) ([]string, error) {

	values := make([]string, len(propInfos))

	for i, info := range propInfos {
		if p := getDSPropertyByName(info.Name, props); p != nil {
			v, err := exp.propertyToString(p.Value, false)
			if err != nil {
				return values, err
			}
			values[i] = v
		} else {
			values[i] = ""
		}
	}
	return values, nil
}

func (exp *CSVExporter) propertyToQuotedString(v interface{}) (str string, err error) {
	s, err := exp.propertyToString(v, true)
	if err != nil {
		return "", err
	}

	switch v.(type) {
	case string:
		str = strconv.Quote(s)
	case []byte:
		str = strconv.Quote(s)
	default:
		str = s
	}
	return
}

func (exp *CSVExporter) propertyToString(v interface{}, quote bool) (str string, err error) {
	switch v := v.(type) {
	case int64:
		str = strconv.FormatInt(v, 10)

	case bool:
		str = strconv.FormatBool(v)

	case string:
		str = v

	case float64:
		str = strconv.FormatFloat(v, 'f', 4, 64)

	case *datastore.Key:
		str = KeyToString(v)

	case time.Time:
		str = v.In(time.UTC).Format(time.RFC3339)

	case datastore.GeoPoint:
		str = fmt.Sprintf("[%v, %v]", v.Lat, v.Lng)

	case []byte:
		str = base64.StdEncoding.EncodeToString(v)

	case *datastore.Entity:
		str = fmt.Sprintf("%v", v.Properties) // TODO

	case []interface{}:
		vals := make([]string, 0)
		for _, e := range v {
			var s string
			if quote {
				s, err = exp.propertyToQuotedString(e)
			} else {
				s, err = exp.propertyToString(e, false)
			}
			if err != nil {
				return "", err
			}
			vals = append(vals, s)
		}
		str = fmt.Sprintf("[%s]", strings.Join(vals, ","))

	case nil:
		str = "null"

	default:
		err = fmt.Errorf("%v is unkown type %v", v, reflect.TypeOf(v))
	}

	return
}
