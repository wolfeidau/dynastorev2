# dynastorev2

This package provides a CRUD (create, read, update and delete) store for [Amazon DynamoDB](https://aws.amazon.com/dynamodb/) using the [AWS Go SDK v2](https://github.com/aws/aws-sdk-go-v2/).

[![Go Report Card](https://goreportcard.com/badge/github.com/wolfeidau/dynastorev2)](https://goreportcard.com/report/github.com/wolfeidau/dynastorev2)
[![Documentation](https://godoc.org/github.com/wolfeidau/dynastorev2?status.svg)](https://godoc.org/github.com/wolfeidau/dynastorev2)

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

	res, err := store.Create(ctx, "customer", "01FCFSDXQ8EYFCNMEA7C2WJG74", []byte(`{"name": "Stax"}`))
	if err != nil {
		// handle error
	}

	// print out the version from the mutation result, this is used for optimistic locking
	fmt.Println("version", res.Version)
```

**Note:** This library doesn't aim to provide a high level abstraction for Amazon DynamoDB, you will need to learn how it works to understand some of the limitations to use it successfully.

# Implementation Tips

Before you get started i recommend you review some of the [code examples](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/service_code_examples.html) provided in the Amazon DynamoDB documentation.

1. Don't use Amazon DynamoDB if you have anything beyond a simple K/V compatible model until you understand your [access patterns](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/bp-modeling-nosql-B.html).
2. Use a [single-table design](https://aws.amazon.com/blogs/compute/creating-a-single-table-design-with-amazon-dynamodb/) to model your data.
3. Use [Universally Unique Lexicographically Sortable Identifier](https://github.com/ulid/spec) (ULID) for sort keys, this will help ensure a rational order of data in the table. Sort key by default is sorted in descending order, oldest first, newest last, exploiting this behaviour may mitigate some of the limitations with Amazon DynamoDB.
4. If your using `WriteWithTTL` you need to deal with the fact that Amazon DynamoDB doesn't delete expired data straight away, records can hang around for up to [48 hours according to the documentation](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/howitworks-ttl.html).

# Status

* [x] Added CRUD with conditional checks and tests
* [x] List with pagination
* [x] [Optimistic Locking](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBMapper.OptimisticLocking.html) for Updates
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