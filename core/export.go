package core

import (
	"fmt"
	"reflect"
	"sort"
	"time"

	"cloud.google.com/go/datastore"
)

type Exporter interface {
	DumpScheme([]*datastore.Key, []datastore.PropertyList) error
	DumpEntities([]*datastore.Key, []datastore.PropertyList) error
}

type PropertyInfo struct {
	Property datastore.Property
	Name     string
	Type     DatastoreType
}

func getPropInfos(entities []datastore.PropertyList) ([]PropertyInfo, error) {

	infoMap := map[string]PropertyInfo{}

	for _, e := range entities {
		props, err := e.Save()
		if err != nil {
			return nil, fmt.Errorf("can not get properties: %v", err)
		}

		for _, p := range props {
			if _, ok := infoMap[p.Name]; !ok {
				dsType, err := getDatastoreType(p.Value)
				if err != nil {
					return nil, fmt.Errorf("can not get properties: %v", err)
				}

				infoMap[p.Name] = PropertyInfo{
					Property: p,
					Name:     p.Name,
					Type:     dsType,
				}
			}
		}
	}

	propInfos := make([]PropertyInfo, len(infoMap))
	i := 0
	for _, v := range infoMap {
		propInfos[i] = v
		i++
	}
	sort.Slice(propInfos, func(i, j int) bool {
		return propInfos[i].Name < propInfos[j].Name
	})

	return propInfos, nil
}

func getDSPropertyByName(name string, props []datastore.Property) *datastore.Property {
	for _, p := range props {
		if p.Name == name {
			return &p
		}
	}
	return nil
}

func getDatastoreType(v interface{}) (DatastoreType, error) {
	switch v.(type) {
	case int64:
		return TypeInteger, nil

	case bool:
		return TypeBool, nil

	case string:
		return TypeString, nil

	case float64:
		return TypeFloat, nil

	case *datastore.Key:
		return TypeKey, nil

	case time.Time:
		return TypeDatetime, nil

	case datastore.GeoPoint:
		return TypeGeo, nil

	case []byte:
		return TypeBlob, nil

	case *datastore.Entity:
		return TypeEmbed, nil

	case []interface{}:
		return TypeArray, nil

	case nil:
		return TypeNull, nil

	default:
		return "", fmt.Errorf("can not convert %v to datastore type", reflect.TypeOf(v).Kind())
	}
}
