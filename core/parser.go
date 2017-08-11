package core

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
