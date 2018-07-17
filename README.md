# dsio

`dsio` is a command line tool for [Google Cloud Datastore](https://cloud.google.com/datastore/). 

**This tool is under development. Please use in your own risk.**

### Features
- Bulk upsert entities from CSV and YAML file.
- Query by GQL from command line.

### Motivation

I have been developing web application which uses Datastore to store application's master data. I want to version-control master data in Datastore, and want to automatically reflect the changes to Datastore using CI service.<br>
like below:

![ci](./docs/ci.png)


# Getting Started
### Installation
```
go get -u github.com/nshmura/dsio
```

### Docker image

1. Create a work directory in your file system
1. Create a Google Cloud Platform service account and store the JSON keys file in a file named `keys.json` in your work dirextory at root level
2. Run dsio with `docker run -it --rm -v /path/to/your/workdir:/workdir ggalmazor/dsio`

Example: List all kinds `docker run -it --rm -v /path/to/your/workdir:/workdir ggalmazor/dsio query --project-id some-project-id 'SELECT * FROM __kinds__'`

### Authentication
1. Create a [service account](https://developers.google.com/identity/protocols/OAuth2ServiceAccount#creatinganaccount). 
2. Set the following environment variable:

 - **DSIO_KEY_FILE** : The path to the JSON key file.
 - **DSIO_PROJECT_ID** : Developers Console project's ID (e.g. bamboo-shift-455)

Or execute `dsio` command with `--key-file` and `--project-id` options:
```
$ dsio upsert --key-file <path-to-service_account_file> --project-id <project-id> ...
```

### How to connect to Datastore Emulator

If you want to connect to [local Datastore emnulator](https://cloud.google.com/datastore/docs/tools/datastore-emulator), execute below command:
```
$(gcloud beta emulators datastore env-init)
```

For more information, please see [this document](https://cloud.google.com/datastore/docs/tools/datastore-emulator#setting_environment_variables).


# Bulk Upsert
To upsert entities from CSV file to Datastore: 
(e.g. upsert into `Book` kind)
```
$ dsio upsert filename.csv -k Book
```


To upsert entities from YAML file to Datastore:
```
$ dsio upsert filename.yaml
```

To specify namespace:
```
$ dsio upsert filename.yaml -n development
```


### File format and Samples:
 - [CSV and TSV format](https://github.com/nshmura/dsio/wiki/CSV-and-TSV-Format)
 - [YAML format](https://github.com/nshmura/dsio/wiki/YAML-Format)
 - [CSV,TSV,YAML file samples](./samples/)


# Query by GQL

To query by [GQL](https://cloud.google.com/datastore/docs/reference/gql_reference):
```
$ dsio query 'SELECT * FROM Book LIMIT 2'
```

Output with CSV format:
```
$ dsio query 'SELECT * FROM Book LIMIT 2' -f csv
```

To specify namespace (e.g. `production` namespace):
```
$ dsio query 'SELECT * FROM Book LIMIT 2' -n production 
```

**CAUTION:** In CSV (and TSV) format, information about types may be dropped in some case, and `noindex` value is removed.
So in some case, there is no way to restore exactly same entities in Datastore from the generated CSV.


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

