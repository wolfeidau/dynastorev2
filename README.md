# dynastorev2

This package provides a CRUD (create, read, update and delete) store for AWS DynamoDB](https://aws.amazon.com/dynamodb/) using the [AWS Go SDK v2](https://github.com/aws/aws-sdk-go-v2/).

# Overview

This is a rewrite of the original [dynastore](https://github.com/wolfeidau/dynastore) with the main differences being:

1. It uses the Generics feature added in Go 1.18
2. It is built on AWS Go SDK v2.
3. The API has been simplified.

# Example

```go
	ctx := context.Background()

	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		// handle error
	}

	client = dynamodb.NewFromConfig(cfg)
	store := dynastorev2.New[string, string, []byte](client, "tickets-table")

	err = store.Create(ctx, "customer", "01FCFSDXQ8EYFCNMEA7C2WJG74", []byte(`{"name": "Stax"}`))
	if err != nil {
		// handle error
	}
```

# Status

* [x] Added CRUD with conditional checks and tests
* [ ] List with pagination
* [ ] [Optimistic Locking](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBMapper.OptimisticLocking.html) for Updates
* [ ] Locking
* [ ] Leasing

# References

Prior work in this space:

* https://github.com/wolfeidau/dynastore
* https://github.com/wolfeidau/dynalock
* https://github.com/awslabs/dynamodb-lock-client
* https://github.com/intercom/lease

Updates to the original API are based on a great blog post by @davecheney https://dave.cheney.net/2014/10/17/functional-options-for-friendly-apis

# License

This code was authored by [Mark Wolfe](https://github.com/wolfeidau) and licensed under the [Apache 2.0 license](http://www.apache.org/licenses/LICENSE-2.0).