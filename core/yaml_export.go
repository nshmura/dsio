package core

import (
	"encoding/base64"
	"fmt"
	"io"
	"time"

	"cloud.google.com/go/datastore"
	"gopkg.in/yaml.v2"
)

type YAMLExport struct {
	writer    io.Writer
	style     TypeStyle
	namespace string
	kind      string
}

func NewYAMLExport(writer io.Writer, style TypeStyle, namespace, kind string) *YAMLExport {
	return &YAMLExport{
		writer:    writer,
		style:     style,
		namespace: namespace,
		kind:      kind,
	}
}

func (exp *YAMLExport) DumpScheme(keys []*datastore.Key, properties []datastore.PropertyList) error {
	propInfos, err := getPropInfos(properties)
	if err != nil {
		return err
	}

	scheme, err := exp.getScheme(propInfos)
	if err != nil {
		return err
	}
	exp.outputYaml("scheme", scheme)

	return nil
}

func (exp *YAMLExport) DumpEntities(keys []*datastore.Key, properties []datastore.PropertyList) error {
	propInfos, err := getPropInfos(properties)
	if err != nil {
		return err
	}

	entities, err := exp.getEntities(keys, properties, propInfos)
	if err != nil {
		return err
	}
	exp.outputYaml("entities", entities)
	return nil
}

func (exp *YAMLExport) getScheme(propInfos []PropertyInfo) (Scheme, error) {

	var scheme Scheme
	if exp.namespace != "" {
		scheme.Namespace = exp.namespace
	}
	if exp.kind != "" {
		scheme.Kind = exp.kind
	}
	if exp.kind != "" {
		scheme.TimeFormat = time.RFC3339
	}

	if exp.style == StyleScheme {
		properties := make(map[string]interface{})
		for _, info := range propInfos {
			t, err := getDatastoreType(info.Property.Value)
			if err != nil {
				return Scheme{}, err
			}
			properties[info.Name] = t
		}
		scheme.Properties = properties
	}

	return scheme, nil
}

func (exp *YAMLExport) getEntities(keys []*datastore.Key, properties []datastore.PropertyList, infos []PropertyInfo) ([]Entity, error) {

	entities := make([]Entity, 0)

	for i, k := range keys {

		entity := make(Entity)
		entity[KeywordKey] = exp.keyValue(k)

		if i < len(properties) {
			props, err := properties[i].Save()
			if err != nil {
				return entities, err
			}
			for _, p := range props {

				property, err := exp.getProperty(infos, p)
				if err != nil {
					return entities, err
				}
				entity[p.Name] = property
			}
		}

		entities = append(entities, entity)
	}
	return entities, nil
}

func (export *YAMLExport) getInfoByPropery(infos []PropertyInfo, p datastore.Property) *PropertyInfo {
	for _, info := range infos {
		if info.Name == p.Name {
			return &info
		}
	}
	return nil
}

func (exp *YAMLExport) getProperty(infos []PropertyInfo, p datastore.Property) (value interface{}, err error) {

	switch exp.style {
	case StyleScheme:
		dsType, err := getDatastoreType(p.Value)
		if err != nil {
			return nil, err
		}

		info := exp.getInfoByPropery(infos, p)
		if info != nil && info.Type == dsType && info.Property.NoIndex == p.NoIndex {
			value, err = exp.getValue(p.Value)
		} else {
			value, err = exp.getDirectTypedValue(p.Value, p.NoIndex)
		}

	case StyleDirect:
		value, err = exp.getDirectTypedValue(p.Value, p.NoIndex)

	case StyleAuto:
		value, err = exp.getAutoTypedValue(p.Value, p.NoIndex)
	}

	return
}

func (exp *YAMLExport) getValueByStyle(val interface{}, noIndex bool) (value interface{}, err error) {

	switch exp.style {
	case StyleScheme:
		value, err = exp.getAutoTypedValue(val, noIndex)

	case StyleDirect:
		value, err = exp.getDirectTypedValue(val, noIndex)

	case StyleAuto:
		value, err = exp.getAutoTypedValue(val, noIndex)
	}
	return
}

func (exp *YAMLExport) getAutoTypedValue(val interface{}, noIndex bool) (value interface{}, err error) {

	switch val.(type) {
	case string, time.Time, int64, float64, bool, []interface{}, *datastore.Entity, nil:
		value, err = exp.getValue(val)
	default:
		value, err = exp.getDirectTypedValue(val, noIndex)
	}
	return
}

func (exp *YAMLExport) getDirectTypedValue(v interface{}, noIndex bool) (interface{}, error) {

	dsType, err := getDatastoreType(v)
	if err != nil {
		return nil, err
	}

	typ := typeKeywordMap[dsType]

	prop, err := exp.getValue(v)
	if err != nil {
		return nil, err
	}

	value := make(map[string]interface{})
	value[typ] = prop
	if noIndex {
		value[KeywordNoIndex] = true
	}
	return value, nil
}

func (exp *YAMLExport) getValue(v interface{}) (value interface{}, err error) {
	switch v := v.(type) {
	case int64:
		value = v

	case bool:
		value = v

	case string:
		value = v

	case float64:
		value = v

	case *datastore.Key:
		value = exp.keyValue(v)

	case time.Time:
		value = v

	case datastore.GeoPoint:
		value = exp.geoPointToValue(v)

	case []byte:
		value = exp.byteToValue(v)

	case *datastore.Entity:
		value, err = exp.entityToValue(v)

	case []interface{}:
		value, err = exp.arrayToValue(v)

	case nil:

	}
	return
}

func (exp *YAMLExport) keyValue(k *datastore.Key) interface{} {

	if k.Parent == nil {
		if k.ID != 0 {
			return k.ID
		} else {
			return k.Name
		}
	}

	keys := make([]interface{}, 0)

	for {
		var v interface{}
		if k.ID != 0 {
			v = k.ID
		} else {
			v = k.Name
		}
		keys = append([]interface{}{k.Kind, v}, keys...)

		if k.Parent == nil {
			return keys
		}
		k = k.Parent
	}
}

func (exp *YAMLExport) geoPointToValue(geo datastore.GeoPoint) []float64 {
	return []float64{geo.Lat, geo.Lng}
}

func (exp *YAMLExport) byteToValue(b []byte) string {
	return base64.StdEncoding.EncodeToString(b)
}

func (exp *YAMLExport) entityToValue(e *datastore.Entity) (map[string]interface{}, error) {

	props := make(map[string]interface{})

	for _, p := range e.Properties {
		value, err := exp.getValueByStyle(p.Value, p.NoIndex)
		if err != nil {
			return props, err
		}
		props[p.Name] = value

	}
	return props, nil
}

func (exp *YAMLExport) arrayToValue(vals []interface{}) ([]interface{}, error) {

	values := make([]interface{}, 0)

	for _, v := range vals {
		value, err := exp.getValueByStyle(v, false)
		if err != nil {
			return values, err
		}
		values = append(values, value)
	}
	return values, nil
}

func (exp *YAMLExport) outputYaml(key string, value interface{}) error {
	output := make(map[string]interface{})
	output[key] = value

	d, err := yaml.Marshal(output)
	if err != nil {
		return err
	}

	fmt.Fprintln(exp.writer, string(d))
	return nil
}
