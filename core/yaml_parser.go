package core

import (
	"encoding/base64"
	"errors"
	"fmt"
	"io/ioutil"
	"regexp"
	"strconv"
	"time"

	"cloud.google.com/go/datastore"
	"gopkg.in/yaml.v2"
)

var (
	errNotDirectTypeValue = errors.New("NotDirectTypeValue")
)

type YAMLParser struct {
	kindData *KindData
}

func NewYAMLParser() *YAMLParser {
	return &YAMLParser{}
}

func (p *YAMLParser) ReadFile(filename string) error {
	source, err := ioutil.ReadFile(filename)
	if err != nil {
		return err
	}

	d := &KindData{}
	if err = yaml.Unmarshal([]byte(source), d); err != nil {
		return err
	}

	p.kindData = d
	return nil
}

func (p *YAMLParser) Validate(ctx Context) error {
	if ctx.Namespace != "" {
		d := p.kindData
		if d.Scheme.Namespace == "" {
			d.Scheme.Namespace = ctx.Namespace
		}
		if d.Scheme.Namespace != ctx.Namespace {
			return fmt.Errorf("different namespace. flag:'%v' != file:'%v'", ctx.Namespace, d.Scheme.Namespace)
		}
	}
	return nil
}

func (p *YAMLParser) Parse() (*[]datastore.Entity, error) {
	d := *p.kindData

	var res []datastore.Entity
	for _, e := range d.Entities {
		if entry, err := p.ParseEntity(e); err != nil {
			return nil, err
		} else {
			res = append(res, entry)
		}
	}
	return &res, nil
}

func (p *YAMLParser) ParseEntity(entity Entity) (dsEntity datastore.Entity, err error) {
	d := *p.kindData

	var key *datastore.Key
	var props []datastore.Property

	// Values
	for name, val := range entity {
		if IsKeyValueName(name) {
			key = p.parseKeyList(val)

		} else {
			if prop, e := p.parseProperty(name, val); e != nil {
				err = e
				return
			} else {
				props = append(props, *prop)
				if key == nil && name == p.kindData.Scheme.Key {
					key = p.parseKeyList(prop.Value)
				}
			}
		}
	}

	// Default Values
	for name, val := range d.Default {
		if IsKeyValueName(name) {
			err = fmt.Errorf("%v can't be as Default value", name)
			return
		}
		if _, ok := entity[name]; !ok {
			if prop, e := p.parseProperty(name, val); e != nil {
				err = e
				return
			} else {
				props = append(props, *prop)
			}
		}
	}

	if key == nil {
		key = p.getDSIncompleteKey(d.Scheme.Kind, nil)
	}

	return datastore.Entity{
		Key:        key,
		Properties: props,
	}, nil
}

func (p *YAMLParser) parseKeyList(val interface{}) *datastore.Key {
	d := p.kindData

	keys, ok := val.([]interface{})

	if !ok {
		return p.parseKey(d.Scheme.Kind, val, nil)
	}

	var key *datastore.Key
	var kind string
	for i, v := range keys {
		if i%2 == 0 {
			kind = ToString(v)
		} else {
			key = p.parseKey(kind, v, key)
		}
	}
	if len(keys)%2 != 0 {
		key = p.getDSIncompleteKey(kind, key)
	}

	return key
}

func (p *YAMLParser) parseKey(kind string, val interface{}, parent *datastore.Key) *datastore.Key {
	switch v := val.(type) {
	case string:
		return p.getDSNamedKey(kind, v, parent)
	case int:
		return p.getDSIDKey(kind, int64(v), parent)
	case int64:
		return p.getDSIDKey(kind, v, parent)
	default:
		panic(Panicf("key should be string or integer: %v", v))
	}
}

func (p *YAMLParser) getDSIDKey(kind string, id int64, parent *datastore.Key) *datastore.Key {
	key := datastore.IDKey(kind, id, parent)
	key.Namespace = p.kindData.Scheme.Namespace
	return key
}

func (p *YAMLParser) getDSNamedKey(kind string, nameKey string, parent *datastore.Key) *datastore.Key {
	key := datastore.NameKey(kind, nameKey, parent)
	key.Namespace = p.kindData.Scheme.Namespace
	return key
}

func (p *YAMLParser) getDSIncompleteKey(kind string, parent *datastore.Key) *datastore.Key {
	key := datastore.IncompleteKey(kind, parent)
	key.Namespace = p.kindData.Scheme.Namespace
	return key
}

func (p *YAMLParser) parseProperty(name string, val interface{}) (*datastore.Property, error) {
	d := p.kindData

	spType, noIndex := p.getTypeInScheme(d.Scheme, name)

	var v interface{}
	var err error
	if spType == "" {
		v, noIndex, err = p.parseValueAutomatically(val)

	} else if m, ok := val.(map[interface{}]interface{}); ok { // Check Directly Specified Types
		v, noIndex, err = p.parseDirectTypeValue(m)
		if err != nil {
			v, err = p.parseValueWithType(DatastoreType(spType), val)
		}

	} else {
		v, err = p.parseValueWithType(DatastoreType(spType), val)
	}

	if err != nil {
		return nil, err
	}

	return &datastore.Property{
		Name:    name,
		Value:   v,
		NoIndex: noIndex,
	}, nil
}

func (p *YAMLParser) getTypeInScheme(scheme Scheme, name string) (string, bool) {
	for k, v := range scheme.Properties {
		if k == name {
			switch v := v.(type) {
			case string:
				return v, false
			case nil:
				return "null", false
			case []interface{}:
				syType := fmt.Sprintf("%v", v[0])
				v2 := fmt.Sprintf("%v", v[1])
				return syType, IsNoIndex(v2)
			}
		}
	}
	return "", false
}

func (p *YAMLParser) parseValueAutomatically(val interface{}) (value interface{}, noIndex bool, err error) {

	switch v := val.(type) {
	case string:
		if t, ok := parseTimestamp(v); ok {
			value = t
		} else {
			value = v
		}
	case int:
		value = int64(v)
	case int64:
		value = v
	case float32:
		value = float64(v)
	case float64:
		value = v
	case bool:
		value = v
	case []interface{}:
		value, err = p.parseArray(v)
	case nil:
		value = v
	case map[interface{}]interface{}:
		value, noIndex, err = p.parseDirectTypeValue(v)
		if err == errNotDirectTypeValue {
			value, err = p.parseEmbed(v)
			noIndex = false
		}

	default:
		err = fmt.Errorf("can't parse value:%v", v)
	}
	return
}

func (p *YAMLParser) parseDirectTypeValue(entry map[interface{}]interface{}) (value interface{}, noIndex bool, err error) {

	noIndexValue, ok := entry[KeywordNoIndex]
	if ok {
		if b, ok2 := noIndexValue.(bool); ok2 {
			noIndex = b
		}
	}

	for k, t := range keywordTypeMap {
		if v, ok := entry[k]; ok {
			value, err = p.parseValueWithType(t, v)
			return
		}
	}

	err = errNotDirectTypeValue
	return
}

func (p *YAMLParser) parseValueWithType(spType DatastoreType, val interface{}) (value interface{}, err error) {
	d := p.kindData

	switch spType {
	case TypeString:
		value = ToString(val)

	case TypeDatetime:
		loc := time.UTC
		if d.Scheme.TimeLocale != "" {
			if loc, err = time.LoadLocation(d.Scheme.TimeLocale); err != nil {
				break
			}
		}

		v := ToString(val)
		if IsCurrentDatetime(v) {
			value = time.Now().In(loc)

		} else {
			if t, e := time.ParseInLocation(d.Scheme.TimeFormat, v, loc); e != nil {
				err = fmt.Errorf("can't parse '%v' to time.", e)
			} else {
				value = t
			}
		}

	case TypeInteger, TypeInt:
		if num, e := strconv.ParseInt(ToString(val), 10, 64); e != nil {
			err = fmt.Errorf("can't parse '%v' to int.", e)
		} else {
			value = num
		}

	case TypeFloat:
		if num, ok := val.(float64); !ok {
			fmt.Errorf("can't parse '%v' to float.", val)
		} else {
			value = num
		}

	case TypeBoolean, TypeBool:
		if num, ok := val.(bool); !ok {
			err = fmt.Errorf("can't parse '%v' to bool.", val)
		} else {
			value = num
		}

	case TypeNull, TypeNil:
		value = nil

	case TypeKey:
		value = p.parseKeyList(val)

	case TypeGeo:
		value = p.parseGeoPoint(val)

	case TypeArray:
		if arr, ok := val.([]interface{}); !ok {
			err = fmt.Errorf("can't parse '%v' to array.", val)
		} else {
			value, err = p.parseArray(arr)
		}

	case TypeBlob:
		blob, ok := val.(string)
		if !ok {
			err = fmt.Errorf("can't parse '%v' to base64 stings.", val)
			break
		}
		if b, e := base64.StdEncoding.DecodeString(blob); e != nil {
			err = fmt.Errorf("can't parse '%v' to base64 stings.(%v)", val, e)
		} else {
			value = b
		}

	case TypeEmbed:
		embed, ok := val.(map[interface{}]interface{})
		if !ok {
			err = fmt.Errorf("can't parse '%v' to embed", val)
		}
		value, err = p.parseEmbed(embed)

	default:
		err = fmt.Errorf("property type '%v' is not supported.", spType)
	}

	return
}

func (p *YAMLParser) parseArray(array []interface{}) ([]interface{}, error) {
	values := make([]interface{}, 0)

	for _, v := range array {
		value, _, err := p.parseValueAutomatically(v)
		if err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, nil
}

func (p *YAMLParser) parseEmbed(embed map[interface{}]interface{}) (*datastore.Entity, error) {
	props := make([]datastore.Property, 0)

	for name, v := range embed {

		value, _, err := p.parseValueAutomatically(v)
		if err != nil {
			return nil, err
		}
		props = append(props, datastore.Property{
			Name:  ToString(name),
			Value: value,
		})
	}

	return &datastore.Entity{
		Properties: props,
	}, nil
}

func parseTimestamp(v interface{}) (time.Time, bool) {
	str, ok := v.(string)
	if !ok {
		return time.Time{}, false
	}
	// 2015-02-24T18:19:39Z
	regxs := map[string]string{
		`^[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]$`: "2006-01-02",
		`^[0-9][0-9][0-9][0-9]` + // (year)
			`-[0-9][0-9]` + // (month)
			`-[0-9][0-9]` + // (day)
			`T[0-9][0-9]` + // (hour)
			`:[0-9][0-9]` + // (minute)
			`:[0-9][0-9]` + // (second)
			`Z|[-+][0-9][0-9]:[0-9][0-9]$`: time.RFC3339, // (time zone)
	}

	for regx, format := range regxs {
		if regexp.MustCompile(regx).MatchString(str) {
			t, err := time.Parse(format, str) // (ymd)
			if err != nil {
				return time.Time{}, false
			}
			return t, true
		}
	}

	return time.Time{}, false
}

func (p *YAMLParser) parseGeoPoint(val interface{}) datastore.GeoPoint {
	geo, ok := val.([]interface{})
	if !ok {
		panic(Panicf("can't parse '%v' to geo point.", val))
	}
	if len(geo) != 2 {
		panic(Panicf("can't parse '%v' to geo point.", val))
	}

	lat, err := ToFloat64(geo[0])
	if err != nil {
		panic(Panicf("can't parse '%v' to geo point.", val))
	}

	lng, err := ToFloat64(geo[1])
	if err != nil {
		panic(Panicf("can't parse '%v' to geo point.", val))
	}

	return datastore.GeoPoint{
		Lat: lat,
		Lng: lng,
	}
}
