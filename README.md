# dsio

`dsio` is a command line tool for [Google Cloud Datastore](https://cloud.google.com/datastore/). 

**This tool is under development. Please use in your own risk.**

### Features
- Bulk upsert entities into Datastore
- Query entities by GQL from Datastore


# Getting Started
### Installation
```
go get -u github.com/nshmura/dsio
```


### Authentication
1. Create [service account](https://cloud.google.com/iam/docs/managing-service-account-keys), and download a JSON file that contains the private key. 
2. Set `DSIO_KEY_FILE` and `DSIO_PROJECT_ID` environment variables. like this:
```
export DSIO_KEY_FILE=/path/to/service_account_file.json
export DSIO_PROJECT_ID=your-gcp-project-id
```

Or set `key-file` option and `project-id` option. <br>
like below `import` command:
```
$ dsio upsert --key-file /path/to/service_account_file.json --project-id your-gcp-project-id
```


# Bulk upsert entities into Datastore
To upsert entities from CSV file: <br>
```
$ dsio upsert filename.csv -f csv -f Book
```


To upsert entities from YAML file: <br>
```
$ dsio upsert filename.yaml -f yaml
```


To specify namespace:
```
$ dsio upsert simple.csv -f csv -n production
```

see:
 - [Samples](./samples/)
 - [CSV format](https://github.com/nshmura/dsio/wiki/CSV-and-TSV-Format)
 - [YAML format](https://github.com/nshmura/dsio/wiki/YAML-Format)


# Query entities by GQL from Datastore

To query entities by [GQL](https://cloud.google.com/datastore/docs/reference/gql_reference):
```
$ dsio query 'SELECT * FROM Book LIMIT 2'
```
Entities are outputed by [YAML format](https://github.com/nshmura/dsio/wiki/YAML-Format).


To specify namespace:
```
$ dsio query 'SELECT * FROM Book LIMIT 2' -n production 
```

To query with CSV format:
```
$ dsio query 'SELECT * FROM Book LIMIT 2' -f csv
```

*In CSV (and TSV) format, all value is converted to string and `noindex` value is disappeared.
So entities exported in CSV (and TSV) format are **different from original entities in Datastore.***

# Options

### dsio upsert
```
$ dsio help upsert

NAME:
   dsio upsert - Bulk-upsert entities into Datastore.

USAGE:
   dsio upsert [command options] filename

OPTIONS:
   --namespace value, -n value  namespace of entities.
   --kind value, -k value       Name of destination kind.
   --format value, -f value     Format of input file. <yaml|csv|tcv>. (default: "yaml")
   --dry-run                    Skip Datastore operations.
   --batch-size value           The number of entities per one multi upsert operation. batch-size should be smaller than 500. (default: 500)
   --key-file value             name of GCP service account file. [$DSIO_KEY_FILE]
   --project-id value           Project ID of GCP. [$DSIO_PROJECT_ID]
   --verbose, -v                Make the operation more talkative.
   --no-color                   Disable color output.

```


### dsio query
```
$ dsio help query

NAME:
   dsio query - Execute a query.

USAGE:
   dsio query [command options] "[<gql_query>]"

OPTIONS:
   --namespace value, -n value  namespace of entities.
   --output value, -o value     Output filename. Entities are outputed into this file.
   --format value, -f value     Format of output. <yaml|csv|tcv>. (default: "yaml")
   --style value, -s value      Style of output. <scheme|direct|auto>. (default: "scheme")
   --page-size value            Number of entities to output at once. (default: 50)
   --key-file value             name of GCP service account file. [$DSIO_KEY_FILE]
   --project-id value           Project ID of GCP. [$DSIO_PROJECT_ID]
   --verbose, -v                Make the operation more talkative.
   --no-color                   Disable color output.
```

