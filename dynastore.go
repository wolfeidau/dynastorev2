package dynastorev2

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	dexp "github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/pkg/errors"
	"golang.org/x/exp/constraints"
	"golang.org/x/exp/slices"
)

const (
	// DefaultPartitionKeyAttribute this is the default partition key attribute name
	DefaultPartitionKeyAttribute = "id"

	// DefaultSortKeyAttribute this is the default sort key attribute name
	DefaultSortKeyAttribute = "name"

	// DefaultExpiresAttribute this is the default name for the dynamodb expiration attribute
	DefaultExpiresAttribute = "expires"

	// DefaultVersionAttribute this is the default name for the dynamodb version attribute used for optimistic locking during creates and atomic updates
	DefaultVersionAttribute = "version"

	// DefaultPayloadAttribute this is the default attribute name containing the encoded payload of the record
	DefaultPayloadAttribute = "payload"
)

var (
	// ErrReservedField extra fields provided have an entry which conflicts with the keys in the table
	ErrReservedField = errors.New("dynastorev2: extra fields contained name which conflicts with table keys attributes")

	// ErrDeleteFailedKeyNotExists delete failed due to constraint added which checks the record exists when deleting
	ErrDeleteFailedKeyNotExists = errors.New("dynastorev2: delete failed as the partition and sort keys didn't exist in the table")
)

// Key ensures the partition or sort key used is a valid type for DynamoDB, note this is also
// referred to as the Primary Key in the AWS documentation.
//
// Each key attribute must be a scalar (meaning that it can hold only a single value).
type Key interface {
	string | constraints.Integer | []byte
}

// Store store using aws sdk v2
type Store[P Key, S Key, V any] struct {
	client       *dynamodb.Client
	tableName    string
	fields       fieldsDef
	storeOptions *storeOptions[P, S, V]
	// writeOptions  *writeOptions[P, S, V]
	// deleteOptions *deleteOptions[P, S]
}

// New creates and configures a new store using aws sdk v2
func New[P Key, S Key, V any](client *dynamodb.Client, tableName string, options ...StoreOption[P, S, V]) *Store[P, S, V] {
	s := &Store[P, S, V]{
		client:    client,
		tableName: tableName,
		fields: fieldsDef{
			partitionKeyName: DefaultPartitionKeyAttribute,
			sortKeyName:      DefaultSortKeyAttribute,
			expiresName:      DefaultExpiresAttribute,
			versionName:      DefaultVersionAttribute,
			payloadName:      DefaultPayloadAttribute,
		},
		storeOptions: &storeOptions[P, S, V]{
			storeHooks: &StoreHooks[P, S, V]{
				RequestBuilt: func(ctx context.Context, pk P, sk S, params any) context.Context {
					return ctx
				},
				ResponseReceived: func(ctx context.Context, pk P, sk S, params any) context.Context {
					return ctx
				},
			},
		},
	}

	applyStoreOptions(s.storeOptions, options...)

	return s
}

// fieldsDef names of the core fields used to manage data in this table
type fieldsDef struct {
	partitionKeyName string
	sortKeyName      string
	expiresName      string
	versionName      string
	payloadName      string
}

// Create a record in DynamoDB using the provided partition and sort keys, a payload containing the value
//
// Note this will use a condition to ensure the specified partition and sort keys don't exist in DynamoDB.
func (t *Store[P, S, V]) Create(ctx context.Context, partitionKey P, sortKey S, value V, options ...WriteOption[P, S, V]) (*OperationResult, error) {

	ctx = setOperationDetails(ctx, "Create", partitionKey, sortKey)

	defaultOpts := t.defaultWriteOptions()
	applyWriteOptions(defaultOpts, options...)

	update, err := t.buildUpdate(value, defaultOpts)
	if err != nil {
		return nil, errors.Wrap(err, "dynastorev2: failed to build update")
	}

	// assign a condition which requires the record to existing before being updated
	createCondition := dexp.AttributeNotExists(dexp.Name(t.fields.partitionKeyName)).And(dexp.AttributeNotExists(dexp.Name(t.fields.sortKeyName)))

	// TODO Add an exclusion for expired records which haven't been cleaned up yet

	expr, err := dexp.NewBuilder().WithUpdate(update).WithCondition(createCondition).Build()
	if err != nil {
		return nil, errors.Wrap(err, "dynastorev2: failed to build update expression")
	}

	result, err := t.doUpdate(ctx, partitionKey, sortKey, value, expr)
	if err != nil {
		return nil, err
	}

	var version int64
	if attr, ok := result.Attributes[t.fields.versionName]; ok {
		err := attributevalue.Unmarshal(attr, &version)
		if err != nil {
			return nil, errors.Wrap(err, "failed to extract version attribute")
		}
	}

	return &OperationResult{
		Version:          version,
		ConsumedCapacity: result.ConsumedCapacity,
	}, nil
}

// Get a record in DynamoDB using the provided partition and sort keys
func (t *Store[P, S, V]) Get(ctx context.Context, partitionKey P, sortKey S, options ...ReadOption[P, S]) (*OperationResult, V, error) {

	var val V

	ctx = setOperationDetails(ctx, "Get", partitionKey, sortKey)

	defaultOpts := t.defaultReadOptions()
	applyReadOptions(defaultOpts, options...)

	key, err := t.buildKey(partitionKey, sortKey)
	if err != nil {
		return nil, val, err
	}

	// TODO Add an exclusion for expired records which haven't been cleaned up yet

	getItem := &dynamodb.GetItemInput{
		TableName:              aws.String(t.tableName),
		Key:                    key,
		ReturnConsumedCapacity: types.ReturnConsumedCapacityTotal,
		ConsistentRead:         aws.Bool(defaultOpts.consistentRead),
	}

	ctx = t.storeOptions.storeHooks.RequestBuilt(ctx, partitionKey, sortKey, getItem)

	readResp, err := t.client.GetItem(ctx, getItem)
	if err != nil {
		return nil, val, errors.Wrap(err, "dynastorev2: failed to get record")
	}

	t.storeOptions.storeHooks.ResponseReceived(ctx, partitionKey, sortKey, readResp.ConsumedCapacity)

	if attr, ok := readResp.Item[t.fields.payloadName]; ok {
		err = attributevalue.Unmarshal(attr, &val)
		if err != nil {
			return nil, val, errors.Wrap(err, "dynastorev2: failed to unmarshal payload attribute")
		}
	}

	var version int64
	if attr, ok := readResp.Item[t.fields.versionName]; ok {
		err := attributevalue.Unmarshal(attr, &version)
		if err != nil {
			return nil, val, errors.Wrap(err, "dynastorev2: failed to extract version attribute")
		}
	}

	return &OperationResult{
		Version:          version,
		ConsumedCapacity: readResp.ConsumedCapacity,
	}, val, nil
}

// ListBySortKeyPrefix perform a query of the DynamoDB using hte partition key and a string prefix
// for the sort key. This is typically used when hierarchies are stored in this partition. For example
// if we have a customer addresses with an sort key with a format of (customer id)/(address id),
// to list the addresses for a customer you list using the customer id as the prefix.
//
// Notes:
// 1. You the sort key must be a string to support this operation, this is a limitation of the AWs SDK.
// 2. ListBySortKeyPrefix will also return expired records as these may hang around for up to 48 hours according to the documentation, see: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/howitworks-ttl.html
func (t *Store[P, S, V]) ListBySortKeyPrefix(ctx context.Context, partitionKey P, prefix string, options ...ReadOption[P, S]) (*OperationResult, []V, error) {
	var vals []V

	ctx = setOperationDetails(ctx, "ListBySortKeyPrefix", partitionKey, prefix)

	defaultOpts := t.defaultReadOptions()
	applyReadOptions(defaultOpts, options...)

	pk, err := attributevalue.Marshal(partitionKey)
	if err != nil {
		return nil, vals, errors.Wrap(err, "dynastorev2: failed to build partition key")
	}

	keyCond := dexp.KeyEqual(dexp.Key(t.fields.partitionKeyName), dexp.Value(pk)).And(dexp.KeyBeginsWith(dexp.Key(t.fields.sortKeyName), prefix))

	expr, err := dexp.NewBuilder().WithKeyCondition(keyCond).Build()
	if err != nil {
		return nil, vals, errors.Wrap(err, "dynastorev2: failed to build list expression")
	}

	queryInput := &dynamodb.QueryInput{
		TableName:                 aws.String(t.tableName),
		ReturnConsumedCapacity:    types.ReturnConsumedCapacityTotal,
		KeyConditionExpression:    expr.KeyCondition(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
	}

	if defaultOpts.lastEvaluatedKey != "" {
		err = parseLastEvaluatedKey(defaultOpts.lastEvaluatedKey, queryInput)
		if err != nil {
			return nil, vals, err
		}
	}

	if defaultOpts.limit > 0 {
		queryInput.Limit = aws.Int32(defaultOpts.limit)
	}

	res, err := t.client.Query(ctx, queryInput)
	if err != nil {
		return nil, vals, errors.Wrap(err, "dynastorev2: failed to execute query")
	}

	for _, item := range res.Items {
		var val V
		err = attributevalue.Unmarshal(item[t.fields.payloadName], &val)
		if err != nil {
			return nil, vals, errors.Wrap(err, "dynastorev2: failed to unmarshal item")
		}

		vals = append(vals, val)
	}

	lastEvaluatedKey, err := encodeLastEvaluatedKey(res)
	if err != nil {
		return nil, vals, err
	}

	return &OperationResult{
		ConsumedCapacity: res.ConsumedCapacity,
		LastEvaluatedKey: lastEvaluatedKey,
	}, vals, nil
}

// Update a record in DynamoDB using the provided partition and sort keys, a payload containing the value
//
// Note this will use a condition to ensure the specified partition and sort keys exist in DynamoDB.
func (t *Store[P, S, V]) Update(ctx context.Context, partitionKey P, sortKey S, value V, options ...WriteOption[P, S, V]) (*OperationResult, error) {

	ctx = setOperationDetails(ctx, "Update", partitionKey, sortKey)

	defaultOpts := t.defaultWriteOptions()
	applyWriteOptions(defaultOpts, options...)

	update, err := t.buildUpdate(value, defaultOpts)
	if err != nil {
		return nil, errors.Wrap(err, "dynastorev2: failed to build update")
	}

	// assign a condition which requires the record to existing before being updated
	updateCondition := dexp.AttributeExists(dexp.Name(t.fields.partitionKeyName)).And(dexp.AttributeExists(dexp.Name(t.fields.sortKeyName)))

	if defaultOpts.version > 0 {
		updateCondition = updateCondition.And(dexp.Equal(dexp.Name(t.fields.versionName), dexp.Value(defaultOpts.version)))
	}

	expr, err := dexp.NewBuilder().WithUpdate(update).WithCondition(updateCondition).Build()
	if err != nil {
		return nil, errors.Wrap(err, "dynastorev2: failed to build update expression")
	}

	result, err := t.doUpdate(ctx, partitionKey, sortKey, value, expr)
	if err != nil {
		return nil, err
	}

	var version int64
	if attr, ok := result.Attributes[t.fields.versionName]; ok {
		err := attributevalue.Unmarshal(attr, &version)
		if err != nil {
			return nil, errors.Wrap(err, "dynastorev2: failed to extract version attribute")
		}
	}

	return &OperationResult{
		ConsumedCapacity: result.ConsumedCapacity,
		Version:          version,
	}, nil
}

// Delete a record in DynamoDB using the provided partition and sort keys
func (t *Store[P, S, V]) Delete(ctx context.Context, partitionKey P, sortKey S, options ...DeleteOption[P, S]) error {
	ctx = setOperationDetails(ctx, "Delete", partitionKey, sortKey)

	defaultOpts := t.defaultDeleteOptions()
	applyDeleteOptions(defaultOpts, options...)

	builder := dexp.NewBuilder()

	// if the delete check is enabled we add a dynamodb attribute exists condition for the partition and sort keys
	if defaultOpts.existsCheck {
		deleteCondition := dexp.AttributeExists(dexp.Name(t.fields.partitionKeyName)).And(dexp.AttributeExists(dexp.Name(t.fields.sortKeyName)))
		builder = builder.WithCondition(deleteCondition)
	}

	expr, err := builder.Build()
	if err != nil {
		return errors.Wrap(err, "dynastorev2: failed to build update expression")
	}

	key, err := t.buildKey(partitionKey, sortKey)
	if err != nil {
		return err
	}

	deleteItem := &dynamodb.DeleteItemInput{
		TableName:                 aws.String(t.tableName),
		Key:                       key,
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		ConditionExpression:       expr.Condition(),
		ReturnConsumedCapacity:    types.ReturnConsumedCapacityTotal,
	}

	ctx = t.storeOptions.storeHooks.RequestBuilt(ctx, partitionKey, sortKey, deleteItem)

	deteteResp, err := t.client.DeleteItem(ctx, deleteItem)
	if err != nil {
		var oe *types.ConditionalCheckFailedException
		if errors.As(err, &oe) {
			return ErrDeleteFailedKeyNotExists
		}

		return errors.Wrap(err, "dynastorev2: failed to delete record")
	}

	t.storeOptions.storeHooks.ResponseReceived(ctx, partitionKey, sortKey, deteteResp.ConsumedCapacity)

	return nil
}

// WriteWithTTL assigns a time to live (TTL) to the record when it is created or updated
func (t *Store[P, S, V]) WriteWithTTL(ttl time.Duration) WriteOption[P, S, V] {
	return writeWithTTL[P, S, V](ttl)

}

// WriteWithVersion adds a condition check the provided version to enable optimistic locking
func (t *Store[P, S, V]) WriteWithVersion(version int64) WriteOption[P, S, V] {
	return writeWithVersion[P, S, V](version)
}

// WriteWithExtraFields assign extra fields provided to the record when written or updated
func (t *Store[P, S, V]) WriteWithExtraFields(extraFields map[string]any) WriteOption[P, S, V] {
	return writeWithExtraFields[P, S, V](extraFields)
}

// ReadWithConsistentRead enable the consistent read flag when performing get operations
func (t *Store[P, S, V]) ReadWithConsistentRead(consistentRead bool) ReadOption[P, S] {
	return readWithConsistentRead[P, S](consistentRead)
}

// ReadWithLastEvaluatedKey provide a last evaluated key when performing list operations
func (t *Store[P, S, V]) ReadWithLastEvaluatedKey(lastEvaluatedKey string) ReadOption[P, S] {
	return readWithLastEvaluatedKey[P, S](lastEvaluatedKey)
}

// ReadWithLimit provide a record count limit when performing list operations
func (t *Store[P, S, V]) ReadWithLimit(limit int32) ReadOption[P, S] {
	return readWithLimit[P, S](limit)
}

// DeleteWithCheck delete with a check condition to ensure the record exists
func (t *Store[P, S, V]) DeleteWithCheck(enabled bool) DeleteOption[P, S] {
	return deleteWithCheck[P, S](enabled)
}

func (t *Store[P, S, V]) doUpdate(ctx context.Context, partitionKey P, sortKey S, value V, expr dexp.Expression) (*dynamodb.UpdateItemOutput, error) {
	key, err := t.buildKey(partitionKey, sortKey)
	if err != nil {
		return nil, err
	}

	updateItem := &dynamodb.UpdateItemInput{
		TableName:                 aws.String(t.tableName),
		Key:                       key,
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		UpdateExpression:          expr.Update(),
		ConditionExpression:       expr.Condition(),
		ReturnConsumedCapacity:    types.ReturnConsumedCapacityTotal,
		ReturnValues:              types.ReturnValueAllNew,
	}

	ctx = t.storeOptions.storeHooks.RequestBuilt(ctx, partitionKey, sortKey, updateItem)

	updateResp, err := t.client.UpdateItem(ctx, updateItem)
	if err != nil {
		return nil, errors.Wrap(err, "dynastorev2: failed to update item")
	}

	t.storeOptions.storeHooks.ResponseReceived(ctx, partitionKey, sortKey, updateResp.ConsumedCapacity)

	return updateResp, nil
}

func (t *Store[P, S, V]) buildKey(partitionKey P, sortKey S) (map[string]types.AttributeValue, error) {

	pk, err := attributevalue.Marshal(partitionKey)
	if err != nil {
		return nil, errors.Wrap(err, "dynastorev2: failed to build partition key")
	}

	sk, err := attributevalue.Marshal(sortKey)
	if err != nil {
		return nil, errors.Wrap(err, "dynastorev2: failed to build sort key")
	}

	return map[string]types.AttributeValue{
		t.fields.partitionKeyName: pk,
		t.fields.sortKeyName:      sk,
	}, nil
}

func (t *Store[P, S, V]) buildUpdate(value V, options *writeOptions[P, S, V]) (dexp.UpdateBuilder, error) {
	// increment the version attribute by one
	update := dexp.Add(dexp.Name(t.fields.versionName), dexp.Value(1))

	val, err := attributevalue.Marshal(value)
	if err != nil {
		return update, errors.Wrap(err, "dynastorev2: failed to marshal value")
	}

	// assign the value to a field called payload
	update = update.Set(dexp.Name("payload"), dexp.Value(val))

	// if we have some additional fields merge those into the top level record as long as they don't match the
	// reserved fields used by the store
	if options.extraFields != nil {
		for k, v := range options.extraFields {
			if t.isReservedField(k) {
				return update, ErrReservedField
			}

			val, err := attributevalue.Marshal(v)
			if err != nil {
				return update, errors.Wrap(err, "dynastorev2: failed to marshal extra field")
			}

			update = update.Set(dexp.Name(k), dexp.Value(val))
		}
	}

	// if a TTL assigned set it, otherwise leave the attribute out so it never expires
	if options.ttl > 0 {
		ttlVal := time.Now().Add(options.ttl).Unix()

		update = update.Set(dexp.Name("expires"), dexp.Value(ttlVal))
	}

	return update, nil
}

func parseLastEvaluatedKey(lastEvaluatedKey string, queryInput *dynamodb.QueryInput) error {
	data, err := base64.RawURLEncoding.DecodeString(lastEvaluatedKey)
	if err != nil {
		return errors.Wrap(err, "dynastorev2: failed to decode last evaluated key")
	}

	m := make(map[string]string)

	err = json.Unmarshal(data, &m)
	if err != nil {
		return errors.Wrap(err, "dynastorev2: failed to unmarshal last evaluated key")
	}

	queryInput.ExclusiveStartKey, err = attributevalue.MarshalMap(&m)
	if err != nil {
		return errors.Wrap(err, "dynastorev2: failed to marshal map into last evaluated key")
	}
	return nil
}

func encodeLastEvaluatedKey(res *dynamodb.QueryOutput) (string, error) {
	if res.LastEvaluatedKey == nil {
		return "", nil
	}

	m := make(map[string]string)
	err := attributevalue.UnmarshalMap(res.LastEvaluatedKey, &m)
	if err != nil {
		return "", errors.Wrap(err, "dynastorev2: failed to unmarshal last evaluated key to map")
	}

	data, err := json.Marshal(&m)
	if err != nil {
		return "", errors.Wrap(err, "dynastorev2: failed to marshal last evaluated key")
	}

	return base64.RawURLEncoding.EncodeToString(data), nil
}

func (t *Store[P, S, V]) isReservedField(k string) bool {
	return slices.Contains([]string{
		t.fields.partitionKeyName,
		t.fields.sortKeyName,
		t.fields.expiresName,
		t.fields.versionName,
		t.fields.payloadName,
	}, k)
}

func (t *Store[P, S, V]) defaultWriteOptions() *writeOptions[P, S, V] {
	return &writeOptions[P, S, V]{
		extraFields: make(map[string]any),
		ttl:         0,
	}
}

func (t *Store[P, S, V]) defaultDeleteOptions() *deleteOptions[P, S] {
	return &deleteOptions[P, S]{
		existsCheck: true,
	}
}

func (t *Store[P, S, V]) defaultReadOptions() *readOptions[P, S] {
	return &readOptions[P, S]{}
}
