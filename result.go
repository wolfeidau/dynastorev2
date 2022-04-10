package dynastorev2

import "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

// OperationResult returned with operations to provide some information about the update
type OperationResult struct {
	Version          int64                   `json:"version,omitempty"`
	ConsumedCapacity *types.ConsumedCapacity `json:"consumed_capacity,omitempty"`
	LastEvaluatedKey string                  `json:"last_evaluated_key,omitempty"`
}
