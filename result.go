package dynastorev2

import "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

// OperationResult returned with operations to provide some information about the update
type OperationResult struct {
	Version          int64
	ConsumedCapacity *types.ConsumedCapacity
}
