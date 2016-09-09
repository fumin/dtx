package dtx

import (
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type failingAmazonDynamodbClient struct {
	*dynamodb.DynamoDB

	getFilter    func(input *dynamodb.GetItemInput) (bool, *dynamodb.GetItemOutput, error)
	putFilter    func(input *dynamodb.PutItemInput) (bool, *dynamodb.PutItemOutput, error)
	updateFilter func(input *dynamodb.UpdateItemInput) (bool, *dynamodb.UpdateItemOutput, error)
}

func (c *failingAmazonDynamodbClient) PutItem(input *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
	if c.putFilter != nil {
		ok, output, err := c.putFilter(input)
		if ok {
			return output, err
		}
	}

	return c.DynamoDB.PutItem(input)
}

func (c *failingAmazonDynamodbClient) GetItem(input *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
	if c.getFilter != nil {
		ok, output, err := c.getFilter(input)
		if ok {
			return output, err
		}
	}

	return c.DynamoDB.GetItem(input)
}

func (c *failingAmazonDynamodbClient) UpdateItem(input *dynamodb.UpdateItemInput) (*dynamodb.UpdateItemOutput, error) {
	if c.updateFilter != nil {
		ok, output, err := c.updateFilter(input)
		if ok {
			return output, err
		}
	}

	return c.DynamoDB.UpdateItem(input)
}
