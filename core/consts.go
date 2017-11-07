package core

const (
	FormatCSV  = "csv"
	FormatTSV  = "tsv"
	FormatYAML = "yaml"
)

const CsvNoIndexKeyword = ":noindex"

type TypeStyle string

const (
	StyleScheme = TypeStyle("scheme")
	StyleDirect = TypeStyle("direct")
	StyleAuto   = TypeStyle("auto")
)

const (
	KeywordKey          = "__key__"
	KeywordCurrent      = "__current__"
	KeywordNoIndex      = "__noindex__"
	KeywordNoIndexValue = "noindex"

	KeywordString   = "__string__"
	KeywordDatetime = "__datetime__"
	KeywordInteger  = "__integer__"
	KeywordInt      = "__int__"
	KeywordFloat    = "__float__"
	KeywordBoolean  = "__boolean__"
	KeywordBool     = "__bool__"
	KeywordGeo      = "__geo__"
	KeywordArray    = "__array__"
	KeywordEmbed    = "__embed__"
	KeywordBlob     = "__blob__"
	KeywordNull     = "__null__"
)

type DatastoreType string

const (
	TypeString   = DatastoreType("string")
	TypeDatetime = DatastoreType("datetime")
	TypeInteger  = DatastoreType("integer")
	TypeInt      = DatastoreType("int")
	TypeFloat    = DatastoreType("float")
	TypeBoolean  = DatastoreType("boolean")
	TypeBool     = DatastoreType("bool")
	TypeKey      = DatastoreType("key")
	TypeGeo      = DatastoreType("geo")
	TypeArray    = DatastoreType("array")
	TypeEmbed    = DatastoreType("embed")
	TypeBlob     = DatastoreType("blob")
	TypeNull     = DatastoreType("null")
	TypeNil      = DatastoreType("<nil>")
)

var (
	keywordTypeMap = map[string]DatastoreType{
		KeywordString:   TypeString,
		KeywordDatetime: TypeDatetime,
		KeywordInteger:  TypeInteger,
		KeywordInt:      TypeInt,
		KeywordFloat:    TypeFloat,
		KeywordBoolean:  TypeBoolean,
		KeywordBool:     TypeBool,
		KeywordKey:      TypeKey,
		KeywordGeo:      TypeGeo,
		KeywordArray:    TypeArray,
		KeywordEmbed:    TypeEmbed,
		KeywordBlob:     TypeBlob,
		KeywordNull:     TypeNull,
	}

	typeKeywordMap = map[DatastoreType]string{
		TypeString:   KeywordString,
		TypeDatetime: KeywordDatetime,
		TypeInteger:  KeywordInteger,
		TypeInt:      KeywordInt,
		TypeFloat:    KeywordFloat,
		TypeBoolean:  KeywordBoolean,
		TypeBool:     KeywordBool,
		TypeKey:      KeywordKey,
		TypeGeo:      KeywordGeo,
		TypeArray:    KeywordArray,
		TypeEmbed:    KeywordEmbed,
		TypeBlob:     KeywordBlob,
		TypeNull:     KeywordNull,
	}
)

func IsKeyValueName(name string) bool {
	return name == KeywordKey
}

func IsCurrentDatetime(name string) bool {
	return name == KeywordCurrent
}

func IsNoIndex(value string) bool {
	return value == KeywordNoIndexValue
}

func IsInt(value string) bool {
	return value == string(TypeInt) || value == string(TypeInteger)
}

func IsArray(value string) bool {
	return value == string(TypeArray)
}
