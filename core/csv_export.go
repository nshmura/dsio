package core

import (
	"encoding/base64"
	"encoding/csv"
	"fmt"
	"io"
	"reflect"
	"strconv"
	"time"

	"cloud.google.com/go/datastore"
)

type CSVExporter struct {
	writer *csv.Writer
	types  map[string]DatastoreType

	schemePropInfos []PropertyInfo
	propInfos       []PropertyInfo
}

func NewCSVExporter(w io.Writer, separator rune) *CSVExporter {

	writer := csv.NewWriter(w)
	writer.Comma = separator

	exp := &CSVExporter{
		writer: writer,
	}

	return exp
}

func (exp *CSVExporter) DumpScheme(keys []*datastore.Key, properties []datastore.PropertyList) error {

	var err error
	if exp.schemePropInfos, err = getPropInfos(properties); err != nil {
		return err
	}

	headers := make([]string, 0, len(exp.schemePropInfos))
	types := make([]string, 0, len(exp.schemePropInfos))

	// append key
	headers = append(headers, KeywordKey)
	if len(keys) > 0 {
		types = append(types, string(GetTypeOfKey(keys[0])))
	}

	for _, info := range exp.schemePropInfos {
		headers = append(headers, info.Name)
		types = append(types, string(info.Type))
	}

	exp.writer.Write(headers)
	exp.writer.Write(types)
	exp.writer.Flush()
	return nil
}

func (exp *CSVExporter) DumpEntities(keys []*datastore.Key, properties []datastore.PropertyList) error {

	propInfos, err := getPropInfos(properties)
	if err != nil {
		return err
	}

	exp.propInfos = exp.appendPropInfos(propInfos)

	for i, e := range properties {
		props, err := e.Save()

		if err != nil {
			return err

		} else {
			values, err := exp.getValueList(exp.propInfos, props)
			if err != nil {
				return err
			}
			values = append([]string{KeyToString(keys[i])}, values...)
			exp.writer.Write(values)
		}
	}

	exp.writer.Flush()
	return nil
}

func (exp *CSVExporter) appendPropInfos(propInfos []PropertyInfo) []PropertyInfo {

	var newInfos []PropertyInfo
	for _, p := range propInfos {
		if !exp.hasSameProperty(exp.propInfos, p) {
			newInfos = append(newInfos, p)
		}
	}

	for _, p := range newInfos {
		exp.propInfos = append(exp.propInfos, p)
	}

	return exp.propInfos
}

func (exp *CSVExporter) hasSameProperty(propInfos []PropertyInfo, p PropertyInfo) bool {
	for _, p2 := range propInfos {
		if p2.Name == p.Name {
			return true
		}
	}
	return false
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
		str = fmt.Sprintf("[%f, %f]", v.Lat, v.Lng)

	case []byte:
		str = base64.StdEncoding.EncodeToString(v)

	case *datastore.Entity:
		vals := make(map[string]interface{})
		for _, p := range v.Properties {
			vals[p.Name] = p.Value
		}
		str, err = EncodeJSON(vals)

	case []interface{}:
		str, err = EncodeJSON(v)

	case nil:
		str = "null"

	default:
		err = fmt.Errorf("%v is unkown type %v", v, reflect.TypeOf(v))
	}

	return
}
