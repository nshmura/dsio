package core

import (
	"bufio"
	"encoding/csv"
	"io"
	"os"
	"regexp"
	"strconv"
	"strings"

	"cloud.google.com/go/datastore"
)

type CSVParser struct {
	parser *Parser

	separator rune
	names     []string
	types     []string
}

func NewCSVParser(separator rune) *CSVParser {
	return &CSVParser{
		parser: &Parser{
			&KindData{},
		},
		separator: separator,
	}
}

func (p *CSVParser) ReadFile(filename string) error {

	f, err := os.Open(filename)
	if err != nil {
		return err
	}

	r := csv.NewReader(bufio.NewReader(f))
	r.Comma = p.separator

	i := 0
	for {
		record, err := r.Read()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}

		switch i {
		case 0:
			p.parsePropertyName(record)
		case 1:
			p.parsePropertyType(record)
		default:
			p.parseEntity(record)
		}
		i++
	}

	return nil
}

func (p *CSVParser) parsePropertyName(record []string) {
	properties := make(map[string]interface{})

	for _, name := range record {
		properties[name] = nil
	}

	p.names = record
	p.parser.kindData.Scheme.Properties = properties
}

func (p *CSVParser) parsePropertyType(record []string) {
	properties := p.parser.kindData.Scheme.Properties

	for i, typ := range record {
		if strings.HasSuffix(typ, CsvNoIndexKeyword) {
			typ = strings.TrimSuffix(typ, CsvNoIndexKeyword)
			properties[p.names[i]] = []string{typ, KeywordNoIndexValue}

		} else if strings.HasPrefix(typ, string(TypeArray)) {
			properties[p.names[i]] = ""

		} else {
			properties[p.names[i]] = typ
		}
		p.types = append(p.types, typ)
	}
	p.parser.kindData.Scheme.Properties = properties
}

func (p *CSVParser) parseEntity(record []string) error {
	entity := Entity{}
	for i, value := range record {

		realType := p.types[i]

		if IsKeyValueName(p.names[i]) {
			typ, _ := p.parser.getTypeInScheme(p.parser.kindData.Scheme, p.names[i])
			if IsInt(typ) {
				v, err := strconv.ParseInt(value, 10, 64)
				if err != nil {
					return err
				}
				entity[p.names[i]] = v

			} else {
				entity[p.names[i]] = value
			}

		} else if IsArray(realType) {
			var list []interface{}
			if entity[p.names[i]] == nil {
				list = make([]interface{},0)
			} else {
				list = entity[p.names[i]].([]interface{})
			}
			entity[p.names[i]] = append(list, value)

		} else if strings.HasPrefix(realType, string(TypeArray)) {

			r := regexp.MustCompile("array\\[([0-9]+)\\]\\.(.*)")
			match := r.FindSubmatch([]byte(realType))
			idx,err := strconv.Atoi(string(match[1]))
			if err != nil {
				return err
			}
			name := string(match[2])

			var list []interface{}
			if entity[p.names[i]] == nil {
				list = make([]interface{},0)
			} else {
				list = entity[p.names[i]].([]interface{})
			}
			if len(list) <= idx {
				list = append(list, make(map[interface{}]interface{}, 0))
			}

			m := list[idx].(map[interface{}]interface{})
			m[name] = value

			if v, err := strconv.ParseInt(value, 10, 64); err == nil {
				m[name] = v
			}
			m[name] = value

			entity[p.names[i]] = list

		} else {
			entity[p.names[i]] = value
		}
	}

	p.parser.kindData.Entities = append(p.parser.kindData.Entities, entity)
	return nil
}

func (p *CSVParser) Parse(kind string) (*[]datastore.Entity, error) {
	if err := p.parser.SetKind(kind); err != nil {
		return nil, err
	}
	if err := p.parser.SetNameSpace(ctx.Namespace); err != nil {
		return nil, err
	}
	if err := p.parser.Validate(ctx); err != nil {
		return nil, err
	}

	d := *p.parser.kindData

	var res []datastore.Entity
	for _, e := range d.Entities {
		if entry, err := p.parser.ParseEntity(e); err != nil {
			return nil, err
		} else {
			res = append(res, entry)
		}
	}

	return &res, nil
}
