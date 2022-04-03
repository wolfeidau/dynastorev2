package dynastorev2

import (
	"time"
)

// StoreOption sets a specific store option
type StoreOption[P Key, S Key, V any] interface {
	apply(opts *storeOptions[P, S, V])
}

// storeOptions holds all available store configuration options
type storeOptions[P Key, S Key, V any] struct {
	storeHooks *StoreHooks[P, S, V]
}

// storeOptionFunc wraps a function and implements the StoreOption interface
type storeOptionFunc[P Key, S Key, V any] func(*storeOptions[P, S, V])

// apply calls the wrapped function.
func (fn storeOptionFunc[P, S, V]) apply(opts *storeOptions[P, S, V]) {
	fn(opts)
}

// applyStoreOptions applies the provided option values to the storeOptions struct
func applyStoreOptions[P Key, S Key, V any](v *storeOptions[P, S, V], opts ...StoreOption[P, S, V]) {
	for i := range opts {
		opts[i].apply(v)
	}
}

func WithStoreHooks[P Key, S Key, V any](storeHooks *StoreHooks[P, S, V]) StoreOption[P, S, V] {
	return storeOptionFunc[P, S, V](func(opts *storeOptions[P, S, V]) {
		opts.storeHooks = storeHooks
	})
}

// Option sets a specific write option
type WriteOption[P Key, S Key, V any] interface {
	apply(opts *writeOptions[P, S, V])
}

// options holds all available write configuration options
type writeOptions[P Key, S Key, V any] struct {
	extraFields map[string]any
	ttl         time.Duration
}

// writeOptionFunc wraps a function and implements the WriteOption interface
type writeOptionFunc[P Key, S Key, V any] func(*writeOptions[P, S, V])

// apply calls the wrapped function
func (fn writeOptionFunc[P, S, V]) apply(opts *writeOptions[P, S, V]) {
	fn(opts)
}

// applyWriteOptions applies the provided option values to the writeOptions struct
func applyWriteOptions[P Key, S Key, V any](v *writeOptions[P, S, V], opts ...WriteOption[P, S, V]) {
	for i := range opts {
		opts[i].apply(v)
	}
}

func WriteWithTTL[P Key, S Key, V any](ttl time.Duration) WriteOption[P, S, V] {
	return writeOptionFunc[P, S, V](func(opts *writeOptions[P, S, V]) {
		opts.ttl = ttl
	})
}

func WriteWithExtraFields[P Key, S Key, V any](extraFields map[string]any) WriteOption[P, S, V] {
	return writeOptionFunc[P, S, V](func(opts *writeOptions[P, S, V]) {
		opts.extraFields = extraFields
	})
}

// readOptions sets a specific read option
type ReadOption[P Key, S Key] interface {
	apply(opts *readOptions[P, S])
}

// readOptions holds all available read configuration options
type readOptions[P Key, S Key] struct {
	consistentRead bool
}

// readOptionFunc wraps a function and implements the ReadOption interface
type readOptionFunc[P Key, S Key] func(*readOptions[P, S])

// apply calls the wrapped function
func (fn readOptionFunc[P, S]) apply(opts *readOptions[P, S]) {
	fn(opts)
}

// applyReadOptions applies the provided option values to the readOptions struct
func applyReadOptions[P Key, S Key](v *readOptions[P, S], opts ...ReadOption[P, S]) {
	for i := range opts {
		opts[i].apply(v)
	}
}

// DeleteOption sets a specific delete option
type DeleteOption[P Key, S Key] interface {
	apply(opts *deleteOptions[P, S])
}

// deleteOptions holds all available delete configuration options
type deleteOptions[P Key, S Key] struct {
	existsCheck bool
}

// deleteOptionFunc wraps a function and implements the DeleteOption interface
type deleteOptionFunc[P Key, S Key] func(*deleteOptions[P, S])

// apply calls the wrapped function
func (fn deleteOptionFunc[P, S]) apply(opts *deleteOptions[P, S]) {
	fn(opts)
}

// applyDeleteOptions applies the provided option values to the deleteOptions struct
func applyDeleteOptions[P Key, S Key](v *deleteOptions[P, S], opts ...DeleteOption[P, S]) {
	for i := range opts {
		opts[i].apply(v)
	}
}

func DeleteWithCheck[P Key, S Key](enabled bool) DeleteOption[P, S] {
	return deleteOptionFunc[P, S](func(opts *deleteOptions[P, S]) {
		opts.existsCheck = enabled
	})
}
