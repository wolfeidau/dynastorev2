package dynastorev2

import (
	"time"
)

// StoreOption sets a specific store option
type StoreOption[P Key, S Key, V any] interface {
	Apply(opts *StoreOptions[P, S, V])
}

// StoreOptions holds all available store configuration options
type StoreOptions[P Key, S Key, V any] struct {
	storeHooks *StoreHooks[P, S, V]
}

// StoreOptionFunc wraps a function and implements the StoreOption interface
type StoreOptionFunc[P Key, S Key, V any] func(*StoreOptions[P, S, V])

// Apply calls the wrapped function.
func (fn StoreOptionFunc[P, S, V]) Apply(opts *StoreOptions[P, S, V]) {
	fn(opts)
}

// ApplyStoreOptions applies the provided option values to the StoreOptions struct
func ApplyStoreOptions[P Key, S Key, V any](v *StoreOptions[P, S, V], opts ...StoreOption[P, S, V]) {
	for i := range opts {
		opts[i].Apply(v)
	}
}

func WithStoreHooks[P Key, S Key, V any](storeHooks *StoreHooks[P, S, V]) StoreOption[P, S, V] {
	return StoreOptionFunc[P, S, V](func(opts *StoreOptions[P, S, V]) {
		opts.storeHooks = storeHooks
	})
}

// Option sets a specific write option
type WriteOption[P Key, S Key, V any] interface {
	Apply(opts *WriteOptions[P, S, V])
}

// options holds all available write configuration options
type WriteOptions[P Key, S Key, V any] struct {
	extraFields              map[string]any
	ttl                      time.Duration
	version                  int64
	createConstraintDisabled bool
}

// WriteOptionFunc wraps a function and implements the WriteOption interface
type WriteOptionFunc[P Key, S Key, V any] func(*WriteOptions[P, S, V])

// Apply calls the wrapped function
func (fn WriteOptionFunc[P, S, V]) Apply(opts *WriteOptions[P, S, V]) {
	fn(opts)
}

// ApplyWriteOptions applies the provided option values to the WriteOptions struct
func ApplyWriteOptions[P Key, S Key, V any](v *WriteOptions[P, S, V], opts ...WriteOption[P, S, V]) {
	for i := range opts {
		opts[i].Apply(v)
	}
}

// WriteWithTTL assigns a time to live (TTL) to the record when it is created or updated
func writeWithTTL[P Key, S Key, V any](ttl time.Duration) WriteOption[P, S, V] {
	return WriteOptionFunc[P, S, V](func(opts *WriteOptions[P, S, V]) {
		opts.ttl = ttl
	})
}

// WriteWithVersion adds a condition check the provided version to enable optimistic locking
func writeWithVersion[P Key, S Key, V any](version int64) WriteOption[P, S, V] {
	return WriteOptionFunc[P, S, V](func(opts *WriteOptions[P, S, V]) {
		opts.version = version
	})
}

// WriteWithExtraFields assign extra fields provided to the record when written or updated
func writeWithExtraFields[P Key, S Key, V any](extraFields map[string]any) WriteOption[P, S, V] {
	return WriteOptionFunc[P, S, V](func(opts *WriteOptions[P, S, V]) {
		opts.extraFields = extraFields
	})
}

// WriteWithCreateConstraintDisabled disable the check on create for existence of the rows
func writeWithCreateConstraintDisabled[P Key, S Key, V any](createConstraintDisabled bool) WriteOption[P, S, V] {
	return WriteOptionFunc[P, S, V](func(opts *WriteOptions[P, S, V]) {
		opts.createConstraintDisabled = createConstraintDisabled
	})
}

// ReadOptions sets a specific read option
type ReadOption[P Key, S Key] interface {
	Apply(opts *ReadOptions[P, S])
}

// ReadOptions holds all available read configuration options
type ReadOptions[P Key, S Key] struct {
	consistentRead   bool
	lastEvaluatedKey string
	limit            int32
}

// ReadOptionFunc wraps a function and implements the ReadOption interface
type ReadOptionFunc[P Key, S Key] func(*ReadOptions[P, S])

// Apply calls the wrapped function
func (fn ReadOptionFunc[P, S]) Apply(opts *ReadOptions[P, S]) {
	fn(opts)
}

// ApplyReadOptions applies the provided option values to the ReadOptions struct
func ApplyReadOptions[P Key, S Key](v *ReadOptions[P, S], opts ...ReadOption[P, S]) {
	for i := range opts {
		opts[i].Apply(v)
	}
}

// readWithConsistentRead enable the consistent read flag when performing get operations
func readWithConsistentRead[P Key, S Key](consistentRead bool) ReadOption[P, S] {
	return ReadOptionFunc[P, S](func(opts *ReadOptions[P, S]) {
		opts.consistentRead = consistentRead
	})
}

// readWithLastEvaluatedKey provide a last evaluated key when performing list operations
func readWithLastEvaluatedKey[P Key, S Key](lastEvaluatedKey string) ReadOption[P, S] {
	return ReadOptionFunc[P, S](func(opts *ReadOptions[P, S]) {
		opts.lastEvaluatedKey = lastEvaluatedKey
	})
}

// readWithLimit provide a record limit when performing list operations
func readWithLimit[P Key, S Key](limit int32) ReadOption[P, S] {
	return ReadOptionFunc[P, S](func(opts *ReadOptions[P, S]) {
		opts.limit = limit
	})
}

// DeleteOption sets a specific delete option
type DeleteOption[P Key, S Key] interface {
	Apply(opts *DeleteOptions[P, S])
}

// DeleteOptions holds all available delete configuration options
type DeleteOptions[P Key, S Key] struct {
	existsCheck bool
}

// deleteOptionFunc wraps a function and implements the DeleteOption interface
type deleteOptionFunc[P Key, S Key] func(*DeleteOptions[P, S])

// Apply calls the wrapped function
func (fn deleteOptionFunc[P, S]) Apply(opts *DeleteOptions[P, S]) {
	fn(opts)
}

// ApplyDeleteOptions applies the provided option values to the DeleteOptions struct
func ApplyDeleteOptions[P Key, S Key](v *DeleteOptions[P, S], opts ...DeleteOption[P, S]) {
	for i := range opts {
		opts[i].Apply(v)
	}
}

func deleteWithCheck[P Key, S Key](enabled bool) DeleteOption[P, S] {
	return deleteOptionFunc[P, S](func(opts *DeleteOptions[P, S]) {
		opts.existsCheck = enabled
	})
}
