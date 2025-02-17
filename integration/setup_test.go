package integration

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/ory/dockertest/v3"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
	"github.com/wolfeidau/dynastorev2"
)

func TestMain(m *testing.M) {

	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.Kitchen}).With().Stack().Caller().Logger()

	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Fatal().Msgf("Could not connect to docker: %s", err)
	}

	// pulls an image, creates a container based on it and runs it
	resource, err := pool.Run("amazon/dynamodb-local", "latest", []string{})
	if err != nil {
		log.Fatal().Err(err).Msg("failed could not start resource")
	}

	endpoint = fmt.Sprintf("http://localhost:%s", resource.GetPort("8000/tcp"))

	// exponential backoff-retry, because the application in the container might not be ready to accept connections yet
	if err := pool.Retry(func() error {

		cfg, err := config.LoadDefaultConfig(context.Background(), config.WithRegion(defaultRegion), config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("123", "123", "123"),
		), config.WithEndpointResolver(aws.EndpointResolverFunc(func(service, region string) (aws.Endpoint, error) {
			return aws.Endpoint{URL: endpoint}, nil
		})))
		if err != nil {
			log.Fatal().Err(err).Msg("failed to configure aws client")
		}

		client = dynamodb.NewFromConfig(cfg)

		_, err = client.ListTables(context.Background(), &dynamodb.ListTablesInput{})
		if err != nil {
			log.Warn().Err(err).Msg("failed to create dynamodb client")
			return err
		}

		log.Info().Msg("client is connected")

		return nil
	}); err != nil {
		log.Fatal().Err(err).Msgf("failed to connect to docker")
	}

	code := m.Run()

	if err := pool.Purge(resource); err != nil {
		log.Fatal().Err(err).Msgf("failed to purge resource")
	}

	os.Exit(code)
}

func ensureTable(ctx context.Context, tableName string) error {

	params := &dynamodb.CreateTableInput{
		TableName: aws.String(tableName),
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("id"), KeyType: types.KeyTypeHash},
			{AttributeName: aws.String("name"), KeyType: types.KeyTypeRange},
		},
		LocalSecondaryIndexes: []types.LocalSecondaryIndex{
			{
				IndexName: aws.String("idx_created"),
				KeySchema: []types.KeySchemaElement{
					{AttributeName: aws.String("id"), KeyType: types.KeyTypeHash},
					{AttributeName: aws.String("created"), KeyType: types.KeyTypeRange},
				},
				Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
			},
		},
		GlobalSecondaryIndexes: []types.GlobalSecondaryIndex{
			{
				IndexName: aws.String("idx_global_1"),
				KeySchema: []types.KeySchemaElement{
					{AttributeName: aws.String("pk1"), KeyType: types.KeyTypeHash},
					{AttributeName: aws.String("sk1"), KeyType: types.KeyTypeRange},
				},
				Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
				ProvisionedThroughput: &types.ProvisionedThroughput{
					ReadCapacityUnits:  aws.Int64(1),
					WriteCapacityUnits: aws.Int64(1),
				},
			},
		},
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("id"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("name"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("created"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("pk1"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("sk1"), AttributeType: types.ScalarAttributeTypeS},
		},
		ProvisionedThroughput: &types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(1),
			WriteCapacityUnits: aws.Int64(1),
		},
		SSESpecification: &types.SSESpecification{
			Enabled: aws.Bool(true),
			SSEType: types.SSETypeAes256,
		},
	}

	_, err := client.CreateTable(ctx, params)
	if err != nil {
		var oe *types.ResourceInUseException
		if errors.As(err, &oe) {
			return nil
		}

		return fmt.Errorf("failed to create table: %w", err)
	}

	err = dynamodb.NewTableExistsWaiter(client).Wait(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	}, 10*time.Second)
	if err != nil {
		return err
	}

	_, err = client.UpdateTimeToLive(ctx, &dynamodb.UpdateTimeToLiveInput{
		TableName: aws.String(tableName),
		TimeToLiveSpecification: &types.TimeToLiveSpecification{
			AttributeName: aws.String("expires"),
			Enabled:       aws.Bool(true),
		},
	})
	if err != nil {
		return err
	}

	return nil
}

func newStore[P dynastorev2.Key, S dynastorev2.Key, V any](t *testing.T) *dynastorev2.Store[P, S, V] {
	assert := require.New(t)
	err := ensureTable(context.Background(), "test-table")
	assert.NoError(err)

	return dynastorev2.New(client, "test-table", dynastorev2.WithStoreHooks(storeHooks[P, S, V]()))
}

func storeHooks[P dynastorev2.Key, S dynastorev2.Key, V any]() *dynastorev2.StoreHooks[P, S, V] {
	return &dynastorev2.StoreHooks[P, S, V]{
		RequestBuilt: func(ctx context.Context, pk P, sk S, params any) context.Context {
			log.Info().Fields(map[string]interface{}{
				"P":      pk,
				"S":      sk,
				"params": params,
			}).Msg("RequestBuilt")
			return ctx
		},
		ResponseReceived: func(ctx context.Context, pk P, sk S, params any) context.Context {
			log.Info().Fields(map[string]interface{}{
				"P":      pk,
				"S":      sk,
				"params": params,
			}).Msg("ResponseReceived")
			return ctx
		},
	}

}

func mustRandKey(len int) string {
	token := make([]byte, len)
	_, err := rand.Read(token)
	if err != nil {
		log.Fatal().Err(err).Msg("mustRandPart failed")
	}

	return hex.EncodeToString(token)
}
