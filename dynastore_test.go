package dynastorev2_test

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/stretchr/testify/require"
	"github.com/wolfeidau/dynastorev2"
)

const (
	defaultRegion = "us-east-1"
	partKeyLen    = 16
)

var (
	client   *dynamodb.Client
	endpoint string
)

type Customer struct {
	ID      string    `json:"id,omitempty"`
	Name    string    `json:"name,omitempty"`
	Created time.Time `json:"created,omitempty"`
}

func TestCreate(t *testing.T) {
	assert := require.New(t)

	store := newStore[string, string, []byte](t)
	part := mustRandKey(partKeyLen)

	res, err := store.Create(context.Background(), part, "sort1", []byte("data"), store.WriteWithTTL(10*time.Second))
	assert.NoError(err)
	assert.Equal(int64(1), res.Version)
}

func TestGet(t *testing.T) {
	assert := require.New(t)

	store := newStore[string, string, []byte](t)
	part := mustRandKey(partKeyLen)

	_, err := store.Create(context.Background(), part, "sort1", []byte("data"), store.WriteWithTTL(10*time.Second))
	assert.NoError(err)

	op, val, err := store.Get(context.Background(), part, "sort1")
	assert.NoError(err)
	assert.Equal([]byte("data"), val)
	assert.Equal(int64(1), op.Version)
}

func TestGetStruct(t *testing.T) {
	assert := require.New(t)

	store := newStore[string, string, Customer](t)
	part := mustRandKey(partKeyLen)

	cust := Customer{ID: mustRandKey(partKeyLen), Name: "test", Created: time.Now().UTC().Round(time.Millisecond)}

	_, err := store.Create(context.Background(), part, cust.ID, cust, store.WriteWithTTL(10*time.Second))
	assert.NoError(err)

	op, val, err := store.Get(context.Background(), part, cust.ID)
	assert.NoError(err)
	assert.Equal(cust, val)
	assert.Equal(int64(1), op.Version)
}

func TestUpdate(t *testing.T) {
	assert := require.New(t)

	store := newStore[string, string, []byte](t)
	part := mustRandKey(partKeyLen)

	op, err := store.Create(context.Background(), part, "sort1", []byte("data"), store.WriteWithTTL(10*time.Second))
	assert.NoError(err)
	assert.Equal(int64(1), op.Version)

	op, err = store.Update(context.Background(), part, "sort1", []byte("data"), store.WriteWithTTL(10*time.Second))
	assert.NoError(err)
	assert.Equal(int64(2), op.Version)
}

func TestUpdateWithExtraFields(t *testing.T) {
	assert := require.New(t)

	store := newStore[string, string, []byte](t)
	part := mustRandKey(partKeyLen)

	_, err := store.Create(context.Background(), part, "sort1", []byte("data"), store.WriteWithTTL(10*time.Second))
	assert.NoError(err)

	_, err = store.Update(context.Background(), part, "sort1", []byte("data"), store.WriteWithTTL(10*time.Second), store.WriteWithExtraFields(
		map[string]any{
			"created": time.Now(),
		},
	))
	assert.NoError(err)
}

func TestUpdateWithFieldsReservedError(t *testing.T) {
	assert := require.New(t)

	store := newStore[string, string, []byte](t)
	part := mustRandKey(partKeyLen)

	_, err := store.Create(context.Background(), part, "sort1", []byte("data"), store.WriteWithTTL(10*time.Second))
	assert.NoError(err)

	_, err = store.Update(context.Background(), part, "sort1", []byte("data"), store.WriteWithTTL(10*time.Second), store.WriteWithExtraFields(
		map[string]any{
			"id": "abc123",
		},
	))
	assert.ErrorAs(err, &dynastorev2.ErrReservedField)
}

func TestUpdateWithVersion(t *testing.T) {
	assert := require.New(t)

	store := newStore[string, string, []byte](t)
	part := mustRandKey(partKeyLen)

	_, err := store.Create(context.Background(), part, "sort1", []byte("data"), store.WriteWithTTL(10*time.Second))
	assert.NoError(err)

	_, err = store.Update(context.Background(), part, "sort1", []byte("data"), store.WriteWithVersion(1))
	assert.NoError(err)

	_, err = store.Update(context.Background(), part, "sort1", []byte("data"), store.WriteWithVersion(100))
	assert.Error(err)
}

func TestDelete(t *testing.T) {
	assert := require.New(t)

	store := newStore[string, string, []byte](t)
	part := mustRandKey(partKeyLen)

	_, err := store.Create(context.Background(), part, "sort1", []byte("data"), store.WriteWithTTL(10*time.Second))
	assert.NoError(err)

	err = store.Delete(context.Background(), part, "sort1")
	assert.NoError(err)

	err = store.Delete(context.Background(), part, "sort1")
	assert.ErrorIs(err, dynastorev2.ErrDeleteFailedKeyNotExists)
}
