package action

import (
	"cloud.google.com/go/datastore"
	"fmt"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
	"strconv"
	"time"
)

const (
	KeyTypeAuto = "auto"
	KeyTypeName = "name"

	TypeString   = "string"
	TypeDatetime = "datetime"
	TypeInteger  = "int"
	TypeFloat    = "float"
	TypeBool     = "boolean"
	TypeKey      = "key"
	TypeGeo      = "geo"
	TypeArray    = "array"
	TypeEmbedded = "embedded"
	TypeNull     = "null"
)

type UpsertFile struct {
	Scheme   Scheme
	Entities []Entity
}

type Scheme struct {
	Namespace string
	Kind      string
	KeyType   string `yaml:"keyType"`
	Locale    string

	Properties []SchemeProperty
}

type SchemeProperty struct {
	Name    string
	Type    string
	NoIndex bool `yaml:"noindex"`
}

type Entity struct {
	Key        string
	Properties []EntityProperty
}

type EntityProperty struct {
	Name   string
	Value  string
	Format string // used for time.ParseInLocation()
}

func Upsert(filename string) {

	source, err := ioutil.ReadFile(filename)
	if err != nil {
		panic(err)
	}

	f := &UpsertFile{}
	err = yaml.Unmarshal([]byte(source), f)
	if err != nil {
		log.Fatalf("error: %v", err)
	}

	fmt.Println(f)

	es := getEntities(f)

	fmt.Println(es)

}

func getEntities(f *UpsertFile) []datastore.Entity {

	var res []datastore.Entity
	for _, pe := range f.Entities {
		res = append(res, getEntity(f.Scheme, pe))
	}
	return res
}

func getEntity(scheme Scheme, entity Entity) datastore.Entity {

	k := getKey(scheme, entity)

	var ps []datastore.Property

	for _, p := range entity.Properties {
		ps = append(ps, getProperty(scheme, p))
	}

	return datastore.Entity{
		Key:        k,
		Properties: ps,
	}
}

func getKey(scheme Scheme, entity Entity) *datastore.Key {

	switch scheme.KeyType {
	case KeyTypeAuto:
		return datastore.IncompleteKey(scheme.Kind, nil) //TODO Parent Key

	case KeyTypeName:
		return datastore.NameKey(scheme.Kind, entity.Key, nil) //TODO Parent Key

	default:
		panic(fmt.Sprintf("key type should one of %v, %v. %v not supported.", KeyTypeAuto, KeyTypeName, scheme.KeyType))
	}
}

func getProperty(scheme Scheme, property EntityProperty) datastore.Property {

	sp := getSchemeProperty(scheme, property)

	return datastore.Property{
		Name:    property.Name,
		Value:   getValue(scheme, sp, property),
		NoIndex: sp.NoIndex,
	}
}

func getSchemeProperty(scheme Scheme, property EntityProperty) SchemeProperty {
	for _, s := range scheme.Properties {
		if s.Name == property.Name {
			return s
		}
	}
	panic(fmt.Sprintf("scheme property of %v not found.", property.Name))
}

func getValue(scheme Scheme, sp SchemeProperty, ep EntityProperty) interface{} {
	switch sp.Type {
	case TypeString:
		return ep.Value

	case TypeDatetime:
		loc, err := time.LoadLocation(scheme.Locale)
		if err != nil {
			panic(fmt.Sprintf("cannot load location from %v.", scheme.Locale))
		}

		t, err := time.ParseInLocation(ep.Format, ep.Value, loc)
		if err != nil {
			panic(fmt.Sprintf("cannot parse time. %v.", err))
		}
		return t

	case TypeInteger:
		num, err := strconv.ParseInt(ep.Value, 10, 64)
		if err != nil {
			panic(fmt.Sprintf("cannot parse int. %v", err))
		}
		return num

	case TypeFloat:
		num, err := strconv.ParseFloat(ep.Value, 64)
		if err != nil {
			panic(fmt.Sprintf("cannot parse int. %v", err))
		}
		return num

	case TypeBool:
		num, err := strconv.ParseBool(ep.Value)
		if err != nil {
			panic(fmt.Sprintf("cannot parse int. %v", err))
		}
		return num

	case TypeNull:
		return nil

	case TypeKey:
	case TypeGeo:
	case TypeArray:
	case TypeEmbedded:
		panic(fmt.Sprintf("sorry... property type %v not supported.", sp.Type))
	}

	panic(fmt.Sprintf("property type %v not supported.", sp.Type))
}
