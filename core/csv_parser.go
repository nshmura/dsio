package core

import (
	"bufio"
	"encoding/csv"
	"io"
	"os"
	"strings"

	"cloud.google.com/go/datastore"
)

type CSVParser struct {
	parser *Parser

	separator rune
	names     []string
}

func NewCSVParser(kind string, separator rune) (*CSVParser, error) {
	p := &CSVParser{
		parser: &Parser{
			&KindData{},
		},
		separator: separator,
	}

	if err := p.parser.SetKind(kind); err != nil {
		return nil, err
	}
	if err := p.parser.SetNameSpace(ctx); err != nil {
		return nil, err
	}
	return p, nil
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
		} else {
			properties[p.names[i]] = typ
		}
	}

	p.parser.kindData.Scheme.Properties = properties
}

func (p *CSVParser) parseEntity(record []string) {
	entity := Entity{}
	for i, value := range record {
		entity[p.names[i]] = value
	}

	p.parser.kindData.Entities = append(p.parser.kindData.Entities, entity)
}

func (p *CSVParser) Parse() (*[]datastore.Entity, error) {
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
