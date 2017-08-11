package core

import (
	"context"

	"cloud.google.com/go/datastore"
	"google.golang.org/api/option"
)

func CreateDatastoreClient(ctx Context) *datastore.Client {

	var opts []option.ClientOption
	if ctx.ServiceAccountFile != "" {
		opts = []option.ClientOption{
			option.WithServiceAccountFile(ctx.ServiceAccountFile),
		}
	}

	client, err := datastore.NewClient(context.Background(), ctx.ProjectID, opts...)
	if err != nil {
		panic(err)
	}

	return client
}
