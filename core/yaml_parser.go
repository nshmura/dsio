package core

import (
	"errors"
	"io/ioutil"

	"cloud.google.com/go/datastore"
	"gopkg.in/yaml.v2"
)

var (
	errNotDirectTypeValue = errors.New("NotDirectTypeValue")
)

type YAMLParser struct {
	parser *Parser
}

func NewYAMLParser(kind string) (*YAMLParser, error) {
	p := &YAMLParser{
		parser: &Parser{
			&KindData{},
		},
	}

	if err := p.parser.SetKind(kind); err != nil {
		return nil, err
	}
	if err := p.parser.SetNameSpace(ctx); err != nil {
		return nil, err
	}

	return p, nil
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

	p.parser.kindData = d
	return nil
}

func (p *YAMLParser) Parse() (*[]datastore.Entity, error) {
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
