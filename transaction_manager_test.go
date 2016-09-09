package dtx

import (
	"fmt"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/pkg/errors"
)

func TestRunInTransactionSucceed(t *testing.T) {
	if err := setup(); err != nil {
		t.Fatalf("%v", err)
	}
	key := newKey(integHashTableName)
	item := make(map[string]*dynamodb.AttributeValue)
	for k, v := range key {
		item[k] = v
	}
	item["attr_s"] = &dynamodb.AttributeValue{S: aws.String("s")}
	input := &dynamodb.PutItemInput{
		TableName: aws.String(integHashTableName),
		Item:      item,
	}

	err := manager.RunInTransaction(func(tx *Transaction) error {
		tx.PutItem(input)
		return nil
	})
	if err != nil {
		t.Fatalf("%v", err)
	}

	if err := assertItemNotLocked(assertItemNotLockedArg{tableName: integHashTableName, key: key, expected: item, shouldExist: true}); err != nil {
		t.Fatalf("%v", err)
	}
}

func TestRunInTransactionFailByUser(t *testing.T) {
	if err := setup(); err != nil {
		t.Fatalf("%v", err)
	}
	key := newKey(integHashTableName)
	item := make(map[string]*dynamodb.AttributeValue)
	for k, v := range key {
		item[k] = v
	}
	item["attr_s"] = &dynamodb.AttributeValue{S: aws.String("s")}
	input := &dynamodb.PutItemInput{
		TableName: aws.String(integHashTableName),
		Item:      item,
	}

	err := manager.RunInTransaction(func(tx *Transaction) error {
		tx.PutItem(input)
		return fmt.Errorf("fail by user")
	})
	if err == nil {
		t.Fatalf("should fail")
	}
	if !strings.Contains(err.Error(), "fail by user") {
		t.Fatalf("wrong err %v", err)
	}

	if err := assertItemNotLocked(assertItemNotLockedArg{tableName: integHashTableName, key: key, shouldExist: false}); err != nil {
		t.Fatalf("%v", err)
	}
}

func TestRunInTransactionFail(t *testing.T) {
	if err := setup(); err != nil {
		t.Fatalf("%v", err)
	}
	key := newKey(integHashTableName)
	item := make(map[string]*dynamodb.AttributeValue)
	for k, v := range key {
		item[k] = v
	}
	item["attr_s"] = &dynamodb.AttributeValue{S: aws.String("s")}
	input := &dynamodb.PutItemInput{
		TableName: aws.String(integHashTableName),
		Item:      item,
	}

	t1Client := &failingAmazonDynamodbClient{DynamoDB: dynamodb.New(sess)}
	t1Client.putFilter = func(input *dynamodb.PutItemInput) (bool, *dynamodb.PutItemOutput, error) {
		if *input.TableName != integHashTableName {
			return false, nil, nil
		}
		return true, nil, fmt.Errorf("failed your request")
	}
	t1Manager := NewTransactionManager(t1Client, integLockTableName, integImagesTableName)
	err := t1Manager.RunInTransaction(func(tx *Transaction) error {
		tx.PutItem(input)
		return nil
	})
	if err == nil {
		t.Fatalf("should fail")
	}
	if !strings.Contains(err.Error(), "failed your request") {
		t.Fatalf("wrong err %v", err)
	}

	if err := assertItemNotLocked(assertItemNotLockedArg{tableName: integHashTableName, key: key, shouldExist: false}); err != nil {
		t.Fatalf("%v", err)
	}
}

func TestInsertNotExists(t *testing.T) {
	if err := setup(); err != nil {
		t.Fatalf("%v", err)
	}
	keyID := randString(8)
	newGetInput := func() *dynamodb.GetItemInput {
		key := make(map[string]*dynamodb.AttributeValue)
		key[idAttribute] = &dynamodb.AttributeValue{S: aws.String(keyID)}
		input := &dynamodb.GetItemInput{
			TableName: aws.String(integHashTableName),
			Key:       key,
		}
		return input
	}
	newPutInput := func(attrVal string) *dynamodb.PutItemInput {
		key := make(map[string]*dynamodb.AttributeValue)
		key[idAttribute] = &dynamodb.AttributeValue{S: aws.String(keyID)}
		item := make(map[string]*dynamodb.AttributeValue)
		for k, v := range key {
			item[k] = v
		}
		item["attr_s"] = &dynamodb.AttributeValue{S: aws.String(attrVal)}
		input := &dynamodb.PutItemInput{
			TableName: aws.String(integHashTableName),
			Item:      item,
		}
		return input
	}

	t1GetOK := make(chan struct{})
	t1Resume := make(chan struct{})
	t1Err := make(chan error)
	go func() {
		t1Err <- manager.RunInTransaction(func(t1 *Transaction) error {
			getResult, err := t1.GetItem(newGetInput())
			if err != nil {
				return err
			}
			if getResult.Item != nil {
				return fmt.Errorf("item should not exists %+v", getResult)
			}

			t1GetOK <- struct{}{}
			<-t1Resume
			t1.PutItem(newPutInput("t1"))
			return nil
		})
	}()

	<-t1GetOK
	err := manager.RunInTransaction(func(t2 *Transaction) error {
		getResult, err := t2.GetItem(newGetInput())
		if err != nil {
			return err
		}
		if getResult.Item != nil {
			return fmt.Errorf("item should not exists %+v", getResult)
		}

		t2.PutItem(newPutInput("t2"))
		return nil
	})
	t1Resume <- struct{}{}
	if err != nil {
		t.Fatalf("%v", err)
	}

	err = <-t1Err
	if err == nil {
		t.Fatalf("t1 should fail")
	}
	if _, ok := errors.Cause(err).(*TransactionRolledBackError); !ok {
		t.Fatalf("wrong error %v", err)
	}

	expKey := newGetInput().Key
	expItem := newPutInput("t2").Item
	if err := assertItemNotLocked(assertItemNotLockedArg{tableName: integHashTableName, key: expKey, expected: expItem, shouldExist: true}); err != nil {
		t.Fatalf("%v", err)
	}
}

func TestQuery(t *testing.T) {
	if err := setup(); err != nil {
		t.Fatalf("%+v", err)
	}
	t1, err := manager.newTransaction()
	if err != nil {
		t.Fatalf("%+v", err)
	}
	item1 := make(map[string]*dynamodb.AttributeValue)
	item1[idAttribute] = &dynamodb.AttributeValue{S: aws.String("a")}
	item1[rangeAttribute] = &dynamodb.AttributeValue{N: aws.String("1")}
	putInput1 := &dynamodb.PutItemInput{
		TableName: aws.String(integRangeTableName),
		Item:      item1,
	}
	if _, err := t1.PutItem(putInput1); err != nil {
		t.Fatalf("%+v", err)
	}
	item3 := make(map[string]*dynamodb.AttributeValue)
	item3[idAttribute] = &dynamodb.AttributeValue{S: aws.String("a")}
	item3[rangeAttribute] = &dynamodb.AttributeValue{N: aws.String("3")}
	putInput3 := &dynamodb.PutItemInput{
		TableName: aws.String(integRangeTableName),
		Item:      item3,
	}
	if _, err := t1.PutItem(putInput3); err != nil {
		t.Fatalf("%+v", err)
	}
	if err := t1.commit(); err != nil {
		t.Fatalf("%+v", err)
	}

	t2, err := manager.newTransaction()
	if err != nil {
		t.Fatalf("%+v", err)
	}
	item2 := make(map[string]*dynamodb.AttributeValue)
	item2[idAttribute] = &dynamodb.AttributeValue{S: aws.String("a")}
	item2[rangeAttribute] = &dynamodb.AttributeValue{N: aws.String("2")}
	item2["some_attr"] = &dynamodb.AttributeValue{S: aws.String("wef")}
	putInput2 := &dynamodb.PutItemInput{
		TableName: aws.String(integRangeTableName),
		Item:      item2,
	}
	if _, err := t2.PutItem(putInput2); err != nil {
		t.Fatalf("%+v", err)
	}

	eav := make(map[string]*dynamodb.AttributeValue)
	eav[":id"] = &dynamodb.AttributeValue{S: aws.String("a")}
	queryInput := &dynamodb.QueryInput{
		TableName:                 aws.String(integRangeTableName),
		ExpressionAttributeValues: eav,
		KeyConditionExpression:    aws.String(fmt.Sprintf("%s = :id", idAttribute)),
		ScanIndexForward:          aws.Bool(false),
	}
	queryOutput1, err := manager.Query(queryInput)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	if err := sliceOfAttributeValueMapEqual(queryOutput1.Items, []map[string]*dynamodb.AttributeValue{item3, item1}); err != nil {
		t.Fatalf("%+v", err)
	}

	if err := t2.commit(); err != nil {
		t.Fatalf("%+v", err)
	}
	queryOutput2, err := manager.Query(queryInput)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	if err := sliceOfAttributeValueMapEqual(queryOutput2.Items, []map[string]*dynamodb.AttributeValue{item3, item2, item1}); err != nil {
		t.Fatalf("%+v", err)
	}

	t2a, err := manager.newTransaction()
	if err != nil {
		t.Fatalf("%+v", err)
	}
	item2a := make(map[string]*dynamodb.AttributeValue)
	item2a[idAttribute] = item2[idAttribute]
	item2a[rangeAttribute] = item2[rangeAttribute]
	item2a["some_attr"] = &dynamodb.AttributeValue{S: aws.String("abcd")}
	putInput2a := &dynamodb.PutItemInput{
		TableName: aws.String(integRangeTableName),
		Item:      item2a,
	}
	if _, err := t2a.PutItem(putInput2a); err != nil {
		t.Fatalf("%+v", err)
	}
	queryOutput2AUnCommit, err := manager.Query(queryInput)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	if err := sliceOfAttributeValueMapEqual(queryOutput2AUnCommit.Items, []map[string]*dynamodb.AttributeValue{item3, item2, item1}); err != nil {
		t.Fatalf("%+v", err)
	}
	if err := sliceOfAttributeValueMapEqual(queryOutput2AUnCommit.Items, []map[string]*dynamodb.AttributeValue{item3, item2a, item1}); err == nil {
		t.Fatalf("should not return item2a %+v", queryOutput2AUnCommit.Items)
	}

	if err := t2a.commit(); err != nil {
		t.Fatalf("%+v", err)
	}
	queryOutput2a, err := manager.Query(queryInput)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	if err := sliceOfAttributeValueMapEqual(queryOutput2a.Items, []map[string]*dynamodb.AttributeValue{item3, item2a, item1}); err != nil {
		t.Fatalf("%+v", err)
	}
}

func sliceOfAttributeValueMapEqual(a, b []map[string]*dynamodb.AttributeValue) error {
	if len(a) != len(b) {
		return fmt.Errorf("different lengths %d %d", len(a), len(b))
	}
	for i, ae := range a {
		be := b[i]
		if err := attributeValueMapEqual(ae, be); err != nil {
			return errors.Wrap(err, fmt.Sprintf("at %d", i))
		}
	}
	return nil
}
