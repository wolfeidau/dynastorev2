package dynastorev2_test

import (
	"context"
	"fmt"

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
	store := dynastorev2.New[string, string, []byte](client, "tickets-table")

	res, err := store.Create(ctx, "customer", "01FCFSDXQ8EYFCNMEA7C2WJG74", []byte(`{"name": "Stax"}`))
	if err != nil {
		// handle error
	}

	// print out the version from the mutation result, this is used for optimistic locking
	fmt.Println("version", res.Version)
}
