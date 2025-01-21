package integration

import (
	"context"
	"fmt"
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

type Address struct {
	ID      string `json:"id,omitempty"`
	Street  string `json:"street,omitempty"`
	Locale  string `json:"locale,omitempty"`
	State   string `json:"state,omitempty"`
	Country string `json:"country,omitempty"`
}

func TestCreate(t *testing.T) {
	assert := require.New(t)

	store := newStore[string, string, []byte](t)
	part := mustRandKey(partKeyLen)

	res, err := store.Create(context.Background(), part, "sort1", []byte("data"), store.WriteWithTTL(10*time.Second))
	assert.NoError(err)
	assert.Equal(int64(1), res.Version)

	res, err = store.Create(context.Background(), part, "sort1", []byte("data"), store.WriteWithTTL(10*time.Second))
	assert.Error(err)
	assert.Nil(res)

	res, err = store.Create(context.Background(), part, "sort1", []byte("data"), store.WriteWithTTL(10*time.Second), store.WriteWithCreateConstraintDisabled(true))
	assert.NoError(err)
	assert.Equal(int64(2), res.Version)
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

	op, val, err = store.Get(context.Background(), part, "sort33")
	assert.Error(err)
	assert.Nil(op)
	assert.Nil(val)
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

func TestListBySortKeyPrefix(t *testing.T) {
	assert := require.New(t)
	ctx := context.Background()

	custStore := newStore[string, string, Customer](t)
	custPart := mustRandKey(partKeyLen)

	cust := Customer{ID: mustRandKey(partKeyLen), Name: "test", Created: time.Now().UTC().Round(time.Millisecond)}

	_, err := custStore.Create(ctx, custPart, cust.ID, cust)
	assert.NoError(err)

	addrStore := newStore[string, string, Address](t)
	addrPart := mustRandKey(partKeyLen)

	addr1 := Address{ID: "a1", Street: "2A George St", Locale: "Brisbane City", State: "Queensland", Country: "Australia"}

	_, err = addrStore.Create(ctx, addrPart, fmt.Sprintf("%s/%s", cust.ID, addr1.ID), addr1)
	assert.NoError(err)

	addr2 := Address{ID: "b2", Street: "2A George St", Locale: "Brisbane City", State: "Queensland", Country: "Australia"}

	_, err = addrStore.Create(ctx, addrPart, fmt.Sprintf("%s/%s", cust.ID, addr2.ID), addr2)
	assert.NoError(err)

	op, vals, err := addrStore.ListBySortKeyPrefix(ctx, addrPart, cust.ID)
	assert.NoError(err)
	assert.Empty(op.LastEvaluatedKey)
	assert.Len(vals, 2)
	assert.Contains(vals, addr1)
	assert.Contains(vals, addr2)

	op, vals, err = addrStore.ListBySortKeyPrefix(ctx, addrPart, cust.ID, addrStore.ReadWithLimit(1))
	assert.NoError(err)
	assert.NotEmpty(op.LastEvaluatedKey)
	assert.Len(vals, 1)
	assert.Contains(vals, addr1)

	op, vals, err = addrStore.ListBySortKeyPrefix(ctx, addrPart, cust.ID, addrStore.ReadWithLastEvaluatedKey(op.LastEvaluatedKey))
	assert.NoError(err)
	assert.Empty(op.LastEvaluatedKey)
	assert.Len(vals, 1)
	assert.Contains(vals, addr2)
}

func TestListBySortKeyPrefixLocalIndex(t *testing.T) {
	assert := require.New(t)
	store := newStore[string, string, []byte](t)
	part := mustRandKey(partKeyLen)

	op, err := store.Create(context.Background(), part, "sort1", []byte("data"), store.WriteWithTTL(10*time.Second), store.WriteWithExtraFields(
		map[string]any{
			"created": "20250101",
		},
	))
	assert.NoError(err)
	assert.Equal(int64(1), op.Version)

	_, results, err := store.ListBySortKeyPrefix(context.Background(), part, "2025", store.ReadWithLimit(1), store.ReadWithIndex("idx_created", "id", "created"))
	assert.NoError(err)
	assert.Len(results, 1)
}

func TestListBySortKeyPrefixGlobalIndex(t *testing.T) {
	assert := require.New(t)
	store := newStore[string, string, []byte](t)
	part := mustRandKey(partKeyLen)

	pk1 := fmt.Sprintf("%s#%s", part, "new") // partition the data in this global index by the partition key

	op, err := store.Create(context.Background(), part, "sort1", []byte("data"), store.WriteWithTTL(10*time.Second), store.WriteWithExtraFields(
		map[string]any{
			"pk1": pk1,
			"sk1": "20250101",
		},
	))
	assert.NoError(err)
	assert.Equal(int64(1), op.Version)

	_, results, err := store.ListBySortKeyPrefix(context.Background(), pk1, "2025", store.ReadWithLimit(1), store.ReadWithIndex("idx_global_1", "pk1", "sk1"))
	assert.NoError(err)
	assert.Len(results, 1)
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
			"created": "20250101",
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
