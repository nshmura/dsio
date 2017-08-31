package core

import (
	"context"

	"cloud.google.com/go/datastore"
	"google.golang.org/api/option"
)

func CreateDatastoreClient(ctx Context) (*datastore.Client, error) {
	var opts []option.ClientOption
	if ctx.ServiceAccountFile != "" {
		opts = []option.ClientOption{
			option.WithCredentialsFile(ctx.ServiceAccountFile),
		}
	}
	return datastore.NewClient(context.Background(), ctx.ProjectID, opts...)
}
