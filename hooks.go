package dynastorev2

import "context"

// StoreHooks is a container for callbacks that can instrument the datastore
type StoreHooks[P Key, S Key, V any] struct {
	// RequestBuilt will be invoked prior to dispatching the request to the AWS SDK
	RequestBuilt     func(ctx context.Context, pk P, sk S, params any) context.Context
	ResponseReceived func(ctx context.Context, pk P, sk S, params any) context.Context
}
