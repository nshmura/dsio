package core

import (
	"encoding/base64"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"time"

	"cloud.google.com/go/datastore"
	"strings"
)

type FileParser interface {
	ReadFile(filename string) error
	Parse() (*[]datastore.Entity, error)
}

type KindData struct {
	Scheme   Scheme   `yaml:"scheme,omitempty"`
	Default  Default  `yaml:"default,omitempty"`
	Entities []Entity `yaml:"entities,omitempty"`
}

type Scheme struct {
	Namespace  string     `yaml:"namespace,omitempty"`
	Kind       string     `yaml:"kind,omitempty"`
	Key        string     `yaml:"key,omitempty"`
	TimeFormat string     `yaml:"time-format,omitempty"` // used for time.ParseInLocation()
	TimeLocale string     `yaml:"time-locale,omitempty"` // used for time.ParseInLocation()
	Properties Properties `yaml:"properties,omitempty"`
}

type Properties map[string]interface{}
type Default map[string]interface{}
type Entity map[string]interface{}

type Parser struct {
	kindData *KindData
}

func (p *Parser) SetKind(optionKind string) error {
	fileKind := p.kindData.Scheme.Kind

	if fileKind == "" {
		p.kindData.Scheme.Kind = optionKind
		return nil
	}

	if optionKind != "" && fileKind != optionKind {
		return fmt.Errorf("kind name unmatched. file:%s, option:%s", fileKind, optionKind)
	}

	p.kindData.Scheme.Kind = optionKind
	return nil
}

func (p *Parser) SetNameSpace(ctx Context) error {
	if ctx.Namespace != "" {
		namespace := p.kindData.Scheme.Namespace
		if namespace == "" {
			namespace = ctx.Namespace
		}
		if namespace != ctx.Namespace {
			return fmt.Errorf("different namespace. flag:'%v' != file:'%v'", ctx.Namespace, namespace)
		}
		p.kindData.Scheme.Namespace = namespace
	}
	return nil
}

func (p *Parser) Validate(ctx Context) error {
	if p.kindData.Scheme.Kind == "" {
		return errors.New("kind should be specified")
	}
	return nil
}

func (p *Parser) ParseEntity(entity Entity) (dsEntity datastore.Entity, err error) {
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
			err = fmt.Errorf("%v cannot be as default value", name)
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

func (p *Parser) parseKeyList(val interface{}) *datastore.Key {
	d := p.kindData

	if s, ok := val.(string); ok {
		if strings.HasPrefix(s, "[") && strings.HasSuffix(s, "]") {
			var arr []interface{}
			if err := DecodeJSON(s, &arr); err != nil {
				return nil
			} else {
				val = arr
			}
		}
	}

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

func (p *Parser) parseKey(kind string, val interface{}, parent *datastore.Key) *datastore.Key {
	switch v := val.(type) {
	case string:
		return p.getDSNamedKey(kind, v, parent)
	case int:
		return p.getDSIDKey(kind, int64(v), parent)
	case int32:
		return p.getDSIDKey(kind, int64(v), parent)
	case int64:
		return p.getDSIDKey(kind, v, parent)
	default:
		panic(Panicf("key should be string or integer: %v", v))
	}
}

func (p *Parser) getDSIDKey(kind string, id int64, parent *datastore.Key) *datastore.Key {
	key := datastore.IDKey(kind, id, parent)
	key.Namespace = p.kindData.Scheme.Namespace
	return key
}

func (p *Parser) getDSNamedKey(kind string, nameKey string, parent *datastore.Key) *datastore.Key {
	key := datastore.NameKey(kind, nameKey, parent)
	key.Namespace = p.kindData.Scheme.Namespace
	return key
}

func (p *Parser) getDSIncompleteKey(kind string, parent *datastore.Key) *datastore.Key {
	key := datastore.IncompleteKey(kind, parent)
	key.Namespace = p.kindData.Scheme.Namespace
	return key
}

func (p *Parser) parseProperty(name string, val interface{}) (*datastore.Property, error) {
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

func (p *Parser) getTypeInScheme(scheme Scheme, name string) (string, bool) {
	for k, v := range scheme.Properties {
		if k == name {
			switch v := v.(type) {
			case string:
				return v, false
			case nil:
				return "null", false
			case []string:
				return v[0], IsNoIndex(v[1])
			case []interface{}:
				return ToString(v[0]), IsNoIndex(ToString(v[1]))
			default:
				Errorf("unsupported error:%v", v)
			}
		}
	}
	return "", false
}

func (p *Parser) parseValueAutomatically(val interface{}) (value interface{}, noIndex bool, err error) {

	switch v := val.(type) {
	case string:
		var loc *time.Location
		loc, err = time.LoadLocation(p.kindData.Scheme.TimeLocale)
		if err != nil {
			return
		}
		if t, ok := p.parseTimestamp(v, loc); ok {
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
		err = fmt.Errorf("can not parse value:%v", v)
	}
	return
}

func (p *Parser) parseDirectTypeValue(entry map[interface{}]interface{}) (value interface{}, noIndex bool, err error) {

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

func (p *Parser) parseValueWithType(spType DatastoreType, val interface{}) (value interface{}, err error) {
	d := p.kindData

	switch spType {
	case TypeString:
		value = ToString(val)

	case TypeDatetime:
		var loc *time.Location
		loc, err = time.LoadLocation(d.Scheme.TimeLocale)
		if err != nil {
			return
		}

		v := ToString(val)
		if IsCurrentDatetime(v) {
			value = time.Now().In(loc)

		} else if d.Scheme.TimeFormat == "" {
			if t, ok := p.parseTimestamp(v, loc); ok {
				value = t
			} else {
				err = fmt.Errorf("can not parse '%v' as time.", v)
			}

		} else {
			if t, e := time.ParseInLocation(d.Scheme.TimeFormat, v, loc); e != nil {
				err = fmt.Errorf("can not parse '%v' as time.", e)
			} else {
				value = t
			}
		}

	case TypeInteger, TypeInt:
		if num, e := strconv.ParseInt(ToString(val), 10, 64); e != nil {
			err = fmt.Errorf("can not parse '%v' as int.", e)
		} else {
			value = num
		}

	case TypeFloat:
		if num, ok := val.(float64); !ok {
			if num, err := strconv.ParseFloat(ToString(val), 64); err != nil {
				err = fmt.Errorf("can not parse '%v' as bool.", val)
			} else {
				value = num
			}
		} else {
			value = num
		}

	case TypeBoolean, TypeBool:
		if num, err := strconv.ParseBool(ToString(val)); err != nil {
			err = fmt.Errorf("can not parse '%v' as bool.", val)
		} else {
			value = num
		}

	case TypeNull, TypeNil:
		value = nil

	case TypeKey:
		value = p.parseKeyList(val)

	case TypeGeo:
		if value, err = p.parseGeoPoint(val); err != nil {
			return
		}

	case TypeArray:
		switch t := val.(type) {
		case []interface{}:
			value, err = p.parseArray(t)

		case string:
			var arr []interface{}
			if err = DecodeJSON(t, &arr); err != nil {
				return
			}
			value, err = p.parseArray(arr)

		default:
			err = fmt.Errorf("can not parse '%v' as array.", val)
		}

	case TypeBlob:
		blob, ok := val.(string)
		if !ok {
			err = fmt.Errorf("can not parse '%v' as base64 stings.", val)
			break
		}
		if b, e := base64.StdEncoding.DecodeString(blob); e != nil {
			err = fmt.Errorf("can not parse '%v' as base64 stings.(%v)", val, e)
		} else {
			value = b
		}

	case TypeEmbed:
		switch t := val.(type) {
		case map[interface{}]interface{}:
			value, err = p.parseEmbed(t)

		case string:
			var json map[string]interface{}
			if err = DecodeJSON(t, &json); err != nil {
				err = fmt.Errorf("can not parse '%v' as json.", json)
			} else {
				embed := make(map[interface{}]interface{})
				for k, v := range json {
					embed[k] = v
				}
				value, err = p.parseEmbed(embed)
			}

		default:
			err = fmt.Errorf("can not parse '%v' as embed.", val)
		}

	default:
		err = fmt.Errorf("property type '%v' is not supported.", spType)
	}

	return
}

func (p *Parser) parseArray(array []interface{}) ([]interface{}, error) {
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

func (p *Parser) parseEmbed(embed map[interface{}]interface{}) (*datastore.Entity, error) {
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

func (p *Parser) parseTimestamp(v interface{}, loc *time.Location) (time.Time, bool) {
	emptyTime := time.Time{}
	str, ok := v.(string)
	if !ok {
		return emptyTime, false
	}

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
			t, err := time.ParseInLocation(format, str, loc) // (ymd)
			if err != nil {
				return emptyTime, false
			}
			return t, true
		}
	}
	return emptyTime, false
}

func (p *Parser) parseGeoPoint(val interface{}) (point datastore.GeoPoint, err error) {

	var geo []interface{}
	switch t := val.(type) {
	case []interface{}:
		geo = t

	case string:
		if err = DecodeJSON(t, &geo); err != nil {
			return
		}
	}

	if geo == nil {
		err = fmt.Errorf("can not parse '%v' as geo.", val)
		return
	}

	if len(geo) != 2 {
		err = fmt.Errorf("can not parse '%v' as geo point.", val)
		return
	}

	lat, err := ToFloat64(geo[0])
	if err != nil {
		err = fmt.Errorf("can not parse '%v' as geo point.", val)
		return
	}

	lng, err := ToFloat64(geo[1])
	if err != nil {
		err = fmt.Errorf("can not parse '%v' as geo point.", val)
		return
	}

	point = datastore.GeoPoint{
		Lat: lat,
		Lng: lng,
	}
	return
}
