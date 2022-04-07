package dynastorev2

import "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

// MutationResult returned with update and create to provide some information about the update
type MutationResult struct {
	Version          int64
	ConsumedCapacity *types.ConsumedCapacity
}
