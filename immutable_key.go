package dtx

import (
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/pkg/errors"

	"github.com/fumin/dtx/omitemptyjson"
)

func toImmutableKey(key map[string]*dynamodb.AttributeValue) (string, error) {
	b, err := omitemptyjson.Marshal(key)
	if err != nil {
		return "", errors.Wrap(err, "Marshal")
	}
	return string(b), nil
}
