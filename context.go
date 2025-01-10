package dynastorev2

import (
	"context"
	"fmt"
)

type operationNameCtxKeyType string

const operationNameCtxKey operationNameCtxKeyType = "operationName"

type OperationDetails struct {
	Name         string
	PartitionKey string
	SortKey      string
}

// OperationName extracts the name of the operation being handled in the given
// context. If it is not known, it returns nil.
func OperationDetailsFromContext(ctx context.Context) *OperationDetails {
	name, _ := ctx.Value(operationNameCtxKey).(*OperationDetails)
	return name
}

func setOperationDetails[P Key, S Key](ctx context.Context, name string, partitionKey P, sortKey S) context.Context {
	return context.WithValue(ctx, operationNameCtxKey, &OperationDetails{
		Name:         name,
		PartitionKey: fmt.Sprint(partitionKey),
		SortKey:      fmt.Sprint(sortKey),
	})
}
