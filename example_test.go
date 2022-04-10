package dynastorev2_test

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/wolfeidau/dynastorev2"
)

func ExampleCreate() {
	ctx := context.Background()

	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		// handle error
	}

	client = dynamodb.NewFromConfig(cfg)
	customerStore := dynastorev2.New[string, string, []byte](client, "tickets-table")

	fields := map[string]any{
		"created": time.Now().UTC().Round(time.Millisecond),
	}

	res, err := customerStore.Create(ctx,
		"customer",                                 // partition key
		"01FCFSDXQ8EYFCNMEA7C2WJG74",               // sort key
		[]byte(`{"name": "Stax"}`),                 // value, in this case JSON encoded value
		customerStore.WriteWithExtraFields(fields), // extra fields which could be indexed in the future
	)
	if err != nil {
		// handle error
	}

	// print out the version from the mutation result, this is used for optimistic locking
	fmt.Println("version", res.Version)
}
