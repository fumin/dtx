package dtx

import (
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/pkg/errors"
)

const (
	awsEndpoint        = "http://127.0.0.1:8000"
	awsRegion          = "us-east-1"
	awsAccessKeyID     = "dtxtestAWSAccessKeyID"
	awsSecretAccessKey = "dtxtestAWSSecretAccessKey"
	awsSessionToken    = "dtxtestAWSSessionToken"

	maxItemSizeBytes = 1024 * 400 // 400kb

	integLockTableName   = "dtxIntegLockTableName"
	txTTLTimeout         = 24 * time.Hour
	integImagesTableName = "dtxIntegImagesTableName"

	integHashTableName  = "dtxIntegHashTableName"
	idAttribute         = "ID"
	integRangeTableName = "dtxIntegRangeTableName"
	rangeAttribute      = "RangeAttr"
)

var (
	sess = session.Must(session.NewSession(&aws.Config{
		Endpoint:    aws.String(awsEndpoint),
		Region:      aws.String(awsRegion),
		Credentials: credentials.NewStaticCredentials(awsAccessKeyID, awsSecretAccessKey, awsSessionToken),
	}))

	manager = NewTransactionManager(&failingAmazonDynamodbClient{DynamoDB: dynamodb.New(sess)}, integLockTableName, integImagesTableName, txTTLTimeout)

	key0  map[string]*dynamodb.AttributeValue
	item0 map[string]*dynamodb.AttributeValue

	jsonMAttrVal map[string]*dynamodb.AttributeValue
)

func deleteTable(client dynamodbiface.DynamoDBAPI, tableName string) (*dynamodb.DeleteTableOutput, error) {
	resp, err := client.DeleteTable(&dynamodb.DeleteTableInput{TableName: aws.String(tableName)})
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			if awsErr.Code() == "ResourceNotFoundException" {
				return nil, nil
			}
		}
		return nil, err
	}
	return resp, nil
}

func initJsonMAttrVal() {
	jsonMAttrVal = make(map[string]*dynamodb.AttributeValue)
	jsonMAttrVal["attr_s"] = &dynamodb.AttributeValue{S: aws.String("s")}
	jsonMAttrVal["attr_n"] = &dynamodb.AttributeValue{N: aws.String("1")}
	jsonMAttrVal["attr_b"] = &dynamodb.AttributeValue{B: []byte("asdf")}
	jsonMAttrVal["attr_ss"] = &dynamodb.AttributeValue{SS: []*string{aws.String("a"), aws.String("b")}}
	jsonMAttrVal["attr_ns"] = &dynamodb.AttributeValue{SS: []*string{aws.String("1"), aws.String("2")}}
	jsonMAttrVal["attr_bs"] = &dynamodb.AttributeValue{BS: [][]byte{[]byte("asdf"), []byte("ghjk")}}
	jsonMAttrVal["attr_bool"] = &dynamodb.AttributeValue{BOOL: aws.Bool(true)}

	attrL := make([]*dynamodb.AttributeValue, 0)
	attrL = append(attrL, &dynamodb.AttributeValue{S: aws.String("s")})
	attrL = append(attrL, &dynamodb.AttributeValue{N: aws.String("1")})
	attrL = append(attrL, &dynamodb.AttributeValue{B: []byte("asdf")})
	attrL = append(attrL, &dynamodb.AttributeValue{BOOL: aws.Bool(true)})
	attrL = append(attrL, &dynamodb.AttributeValue{NULL: aws.Bool(true)})
	jsonMAttrVal["attr_l"] = &dynamodb.AttributeValue{L: attrL}

	jsonMAttrVal["attr_null"] = &dynamodb.AttributeValue{NULL: aws.Bool(true)}
}

func setupCreateItem() error {
	tx, err := manager.newTransaction()
	if err != nil {
		return fmt.Errorf("NewTransaction %v", err)
	}
	key0 = newKey(integHashTableName)
	item0 = make(map[string]*dynamodb.AttributeValue)
	for k, v := range key0 {
		item0[k] = v
	}
	item0["s_someattr"] = &dynamodb.AttributeValue{S: aws.String("val")}
	item0["ss_someattr"] = &dynamodb.AttributeValue{SS: []*string{aws.String("one"), aws.String("two")}}
	item0["n_someattr"] = &dynamodb.AttributeValue{N: aws.String("1473433292.083")}
	item0["ns_someattr"] = &dynamodb.AttributeValue{NS: []*string{aws.String("1473433337.887"), aws.String("47.23")}}
	putInput := &dynamodb.PutItemInput{
		TableName: aws.String(integHashTableName),
		Item:      item0,
	}
	_, err = tx.PutItem(putInput)
	if err != nil {
		return fmt.Errorf("PutItem %v", err)
	}
	if err := tx.commit(); err != nil {
		return fmt.Errorf("Commit %v", err)
	}
	if err := assertItemNotLocked(assertItemNotLockedArg{tableName: integHashTableName, key: key0, expected: item0, shouldExist: true}); err != nil {
		return fmt.Errorf("assertItemNotLocked %v", err)
	}
	return nil
}

func setup() error {
	backoffDefaultUnit = time.Nanosecond
	backoffDefaultMaxWaitTime = time.Millisecond

	if _, err := deleteTable(manager.client, integLockTableName); err != nil {
		return fmt.Errorf("deleteTable %s %v", integLockTableName, err)
	}
	if _, err := deleteTable(manager.client, integImagesTableName); err != nil {
		return fmt.Errorf("deleteTable %s %v", integImagesTableName, err)
	}
	if _, err := deleteTable(manager.client, integHashTableName); err != nil {
		return fmt.Errorf("DeleteTable %s %v", integHashTableName, err)
	}
	if _, err := deleteTable(manager.client, integRangeTableName); err != nil {
		return fmt.Errorf("DeleteTable %s %v", integRangeTableName, err)
	}
	if err := manager.VerifyOrCreateTransactionTable(); err != nil {
		return fmt.Errorf("VerifyOrCreateTransactionTable %v", err)
	}
	if err := manager.VerifyOrCreateTransactionImagesTable(); err != nil {
		return fmt.Errorf("VerifyOrCreateTransactionImagesTable %v", err)
	}
	if err := verifyOrCreateTable(manager.client, integHashTableName, []*dynamodb.AttributeDefinition{{AttributeName: aws.String(idAttribute), AttributeType: aws.String("S")}}, []*dynamodb.KeySchemaElement{{AttributeName: aws.String(idAttribute), KeyType: aws.String("HASH")}}); err != nil {
		return fmt.Errorf("create hash table %v", err)
	}
	if err := verifyOrCreateTable(manager.client, integRangeTableName, []*dynamodb.AttributeDefinition{{AttributeName: aws.String(idAttribute), AttributeType: aws.String("S")}, {AttributeName: aws.String(rangeAttribute), AttributeType: aws.String("N")}}, []*dynamodb.KeySchemaElement{{AttributeName: aws.String(idAttribute), KeyType: aws.String("HASH")}, {AttributeName: aws.String(rangeAttribute), KeyType: aws.String("RANGE")}}); err != nil {
		return fmt.Errorf("create range table %v", err)
	}

	client := manager.client.(*failingAmazonDynamodbClient)
	client.getFilter = nil
	client.putFilter = nil
	client.updateFilter = nil

	if err := setupCreateItem(); err != nil {
		return fmt.Errorf("setupCreateItem %v", err)
	}

	initJsonMAttrVal()
	return nil
}

func TestPhantomItemFromDelete(t *testing.T) {
	if err := setup(); err != nil {
		t.Fatalf("%v", err)
	}
	key1 := newKey(integHashTableName)
	transaction, err := manager.newTransaction()
	if err != nil {
		t.Fatalf("%v", err)
	}
	deleteRequest := &dynamodb.DeleteItemInput{
		TableName: aws.String(integHashTableName),
		Key:       key1,
	}
	if _, err := transaction.DeleteItem(deleteRequest); err != nil {
		t.Fatalf("%v", err)
	}

	// Although, the item did not exist, it is nonetheless created to hold the lock.
	if err := assertItemLocked(assertItemLockedArg{tableName: integHashTableName, key: key1, owner: transaction.ID(), isTransient: true, checkTxItem: true}); err != nil {
		t.Fatalf("%v", err)
	}

	if err := rollback(transaction); err != nil {
		t.Fatalf("%v", err)
	}

	// Check that the transient item has been deleted.
	if err := assertItemNotLocked(assertItemNotLockedArg{tableName: integHashTableName, key: key1, shouldExist: false}); err != nil {
		t.Fatalf("%v", err)
	}
}

func TestLockItem(t *testing.T) {
	if err := setup(); err != nil {
		t.Fatalf("%v", err)
	}
	key1 := newKey(integHashTableName)
	t1, err := manager.newTransaction()
	if err != nil {
		t.Fatalf("%v", err)
	}
	t2, err := manager.newTransaction()
	if err != nil {
		t.Fatalf("%v", err)
	}

	lockRequest := &dynamodb.GetItemInput{
		TableName: aws.String(integHashTableName),
		Key:       key1,
	}
	getResult, err := t1.GetItem(lockRequest)
	if err != nil {
		t.Fatalf("%v", err)
	}
	if getResult.Item != nil {
		t.Fatalf("item should not exists %+v", getResult)
	}
	// we're not applying locks, isApplied is false.
	if err := assertItemLocked(assertItemLockedArg{tableName: integHashTableName, key: key1, owner: t1.ID(), isTransient: true, isApplied: false, checkTxItem: true}); err != nil {
		t.Fatalf("%v", err)
	}

	// Do a transaction in a middle of t1, and hence rollback t1.
	deleteRequest := &dynamodb.DeleteItemInput{
		TableName: aws.String(integHashTableName),
		Key:       key1,
	}
	deleteResult, err := t2.DeleteItem(deleteRequest)
	if err != nil {
		t.Fatalf("%v", err)
	}
	if deleteResult.Attributes != nil {
		t.Fatalf("item should not exists %+v", deleteResult)
	}
	// we're not applying locks, isApplied is false.
	if err := assertItemLocked(assertItemLockedArg{tableName: integHashTableName, key: key1, owner: t2.ID(), isTransient: true, isApplied: false, checkTxItem: true}); err != nil {
		t.Fatalf("%v", err)
	}

	if err := t2.commit(); err != nil {
		t.Fatalf("t2 should succeed %v", err)
	}
	if err := t1.commit(); err == nil {
		t.Fatalf("t1 should fail")
	}

	if err := assertItemNotLocked(assertItemNotLockedArg{tableName: integHashTableName, key: key1, shouldExist: false}); err != nil {
		t.Fatalf("%v", err)
	}
}

func TestLock2Items(t *testing.T) {
	if err := setup(); err != nil {
		t.Fatalf("%v", err)
	}
	key1 := newKey(integHashTableName)
	key2 := newKey(integHashTableName)

	t0, err := manager.newTransaction()
	if err != nil {
		t.Fatalf("%v", err)
	}
	item1 := make(map[string]*dynamodb.AttributeValue)
	for k, v := range key1 {
		item1[k] = v
	}
	item1["something"] = &dynamodb.AttributeValue{S: aws.String("val")}
	putInput := &dynamodb.PutItemInput{
		TableName: aws.String(integHashTableName),
		Item:      item1,
	}
	_, err = t0.PutItem(putInput)
	if err != nil {
		t.Fatalf("%v", err)
	}
	if err := t0.commit(); err != nil {
		t.Fatalf("%v", err)
	}

	t1, err := manager.newTransaction()
	if err != nil {
		t.Fatalf("%v", err)
	}
	getInput1 := &dynamodb.GetItemInput{
		TableName: aws.String(integHashTableName),
		Key:       key1,
	}
	getResult1, err := t1.GetItem(getInput1)
	if err != nil {
		t.Fatalf("%v", err)
	}
	if err := assertItemLocked(assertItemLockedArg{tableName: integHashTableName, key: key1, owner: t1.ID(), isTransient: false, isApplied: false, checkTxItem: true}); err != nil {
		t.Fatalf("%v", err)
	}
	if err := attributeValueMapEqual(getResult1.Item, item1); err != nil {
		t.Fatalf("attributeValueMapEqual %v", err)
	}

	getInput2 := &dynamodb.GetItemInput{
		TableName: aws.String(integHashTableName),
		Key:       key2,
	}
	getResult2, err := t1.GetItem(getInput2)
	if err != nil {
		t.Fatalf("%v", err)
	}
	if err := assertItemLocked(assertItemLockedArg{tableName: integHashTableName, key: key1, owner: t1.ID(), isTransient: false, isApplied: false, checkTxItem: true}); err != nil {
		t.Fatalf("%v", err)
	}
	if err := assertItemLocked(assertItemLockedArg{tableName: integHashTableName, key: key2, owner: t1.ID(), isTransient: true, isApplied: false, checkTxItem: true}); err != nil {
		t.Fatalf("%v", err)
	}
	if getResult2.Item != nil {
		t.Fatalf("should get no result %+v", getResult2)
	}

	if err := t1.commit(); err != nil {
		t.Fatalf("%v", err)
	}

	if err := assertItemNotLocked(assertItemNotLockedArg{tableName: integHashTableName, key: key1, expected: item1, shouldExist: true}); err != nil {
		t.Fatalf("%v", err)
	}
	if err := assertItemNotLocked(assertItemNotLockedArg{tableName: integHashTableName, key: key2, shouldExist: false}); err != nil {
		t.Fatalf("%v", err)
	}
}

func TestGetItemNotExists(t *testing.T) {
	if err := setup(); err != nil {
		t.Fatalf("%v", err)
	}
	t1, err := manager.newTransaction()
	if err != nil {
		t.Fatalf("%v", err)
	}
	key1 := newKey(integHashTableName)

	getRequest := &dynamodb.GetItemInput{
		TableName: aws.String(integHashTableName),
		Key:       key1,
	}
	getResult1, err := t1.GetItem(getRequest)
	if err != nil {
		t.Fatalf("%v", err)
	}
	if getResult1.Item != nil {
		t.Fatalf("item should not exists %+v", getResult1)
	}
	if err := assertItemLocked(assertItemLockedArg{tableName: integHashTableName, key: key1, owner: t1.ID(), isTransient: true, isApplied: false, checkTxItem: true}); err != nil {
		t.Fatalf("%v", err)
	}

	getResult2, err := t1.GetItem(getRequest)
	if err != nil {
		t.Fatalf("%v", err)
	}
	if getResult2.Item != nil {
		t.Fatalf("item should not exists %+v", getResult2)
	}
	if err := assertItemLocked(assertItemLockedArg{tableName: integHashTableName, key: key1, owner: t1.ID(), isTransient: true, isApplied: false, checkTxItem: true}); err != nil {
		t.Fatalf("%v", err)
	}

	if err := t1.commit(); err != nil {
		t.Fatalf("%v", err)
	}

	if err := assertItemNotLocked(assertItemNotLockedArg{tableName: integHashTableName, key: key1, shouldExist: false}); err != nil {
		t.Fatalf("%v", err)
	}
}

func TestGetThenUpdateNewItem(t *testing.T) {
	if err := setup(); err != nil {
		t.Fatalf("%v", err)
	}
	t1, err := manager.newTransaction()
	if err != nil {
		t.Fatalf("%v", err)
	}
	key1 := newKey(integHashTableName)
	item1 := make(map[string]*dynamodb.AttributeValue)
	for k, v := range key1 {
		item1[k] = v
	}
	item1["asdf"] = &dynamodb.AttributeValue{S: aws.String("did not exist")}

	getRequest := &dynamodb.GetItemInput{
		TableName: aws.String(integHashTableName),
		Key:       key1,
	}
	getResult1, err := t1.GetItem(getRequest)
	if err != nil {
		t.Fatalf("%v", err)
	}
	if getResult1.Item != nil {
		t.Fatalf("item should not exists %+v", getResult1)
	}
	if err := assertItemLocked(assertItemLockedArg{tableName: integHashTableName, key: key1, owner: t1.ID(), isTransient: true, isApplied: false, checkTxItem: true}); err != nil {
		t.Fatalf("%v", err)
	}

	eav := make(map[string]*dynamodb.AttributeValue)
	sets := make([]string, 0, len(item1))
	for k, v := range item1 {
		if _, ok := key1[k]; ok {
			continue
		}
		exprName := fmt.Sprintf(":%s", k)
		eav[exprName] = v
		sets = append(sets, fmt.Sprintf("%s = %s", k, exprName))
	}
	updateExpr := fmt.Sprintf("SET %s", strings.Join(sets, ", "))
	updateInput := &dynamodb.UpdateItemInput{
		TableName: aws.String(integHashTableName),
		Key:       key1,
		ExpressionAttributeValues: eav,
		UpdateExpression:          &updateExpr,
		ReturnValues:              aws.String("ALL_NEW"),
	}
	updateResult, err := t1.UpdateItem(updateInput)
	if err != nil {
		t.Fatalf("%v", err)
	}
	if err := assertItemLocked(assertItemLockedArg{tableName: integHashTableName, key: key1, owner: t1.ID(), isTransient: true, isApplied: true, checkTxItem: true}); err != nil {
		t.Fatalf("%v", err)
	}
	if err := attributeValueMapEqual(updateResult.Attributes, item1); err != nil {
		t.Fatalf("attributeValueMapEqual %v", err)
	}

	if err := t1.commit(); err != nil {
		t.Fatalf("%v", err)
	}

	if err := assertItemNotLocked(assertItemNotLockedArg{tableName: integHashTableName, key: key1, expected: item1, shouldExist: true}); err != nil {
		t.Fatalf("%v", err)
	}
}

func TestGetThenUpdateExistingItem(t *testing.T) {
	if err := setup(); err != nil {
		t.Fatalf("%v", err)
	}
	t1, err := manager.newTransaction()
	if err != nil {
		t.Fatalf("%v", err)
	}
	getInput := &dynamodb.GetItemInput{
		TableName: aws.String(integHashTableName),
		Key:       key0,
	}
	getResult, err := t1.GetItem(getInput)
	if err != nil {
		t.Fatalf("%v", err)
	}
	if err := assertItemLocked(assertItemLockedArg{tableName: integHashTableName, key: key0, owner: t1.ID(), isTransient: false, isApplied: false, checkTxItem: true}); err != nil {
		t.Fatalf("%v", err)
	}
	if err := attributeValueMapEqual(getResult.Item, item0); err != nil {
		t.Fatalf("attributeValueMapEqual %v", err)
	}

	item0a := make(map[string]*dynamodb.AttributeValue)
	for k, v := range item0 {
		item0a[k] = v
	}
	item0a["wef"] = &dynamodb.AttributeValue{S: aws.String("new_attr")}
	eav := make(map[string]*dynamodb.AttributeValue)
	eav[":wef"] = item0a["wef"]
	updateExpr := aws.String("SET wef = :wef")
	updateInput := &dynamodb.UpdateItemInput{
		TableName: aws.String(integHashTableName),
		Key:       key0,
		ExpressionAttributeValues: eav,
		UpdateExpression:          updateExpr,
		ReturnValues:              aws.String("ALL_NEW"),
	}
	updateResult, err := t1.UpdateItem(updateInput)
	if err != nil {
		t.Fatalf("%v", err)
	}
	if err := assertItemLocked(assertItemLockedArg{tableName: integHashTableName, key: key0, owner: t1.ID(), isTransient: false, isApplied: true, checkTxItem: true}); err != nil {
		t.Fatalf("%v", err)
	}
	if err := attributeValueMapEqual(updateResult.Attributes, item0a); err != nil {
		t.Fatalf("attributeValueMapEqual %v", err)
	}

	if err := t1.commit(); err != nil {
		t.Fatalf("%v", err)
	}

	if err := assertItemNotLocked(assertItemNotLockedArg{tableName: integHashTableName, key: key0, expected: item0a, shouldExist: true}); err != nil {
		t.Fatalf("%v", err)
	}
}

func TestConflictingWrites(t *testing.T) {
	if err := setup(); err != nil {
		t.Fatalf("%v", err)
	}
	t1, err := manager.newTransaction()
	if err != nil {
		t.Fatalf("%v", err)
	}
	t2, err := manager.newTransaction()
	if err != nil {
		t.Fatalf("%v", err)
	}
	t3, err := manager.newTransaction()
	if err != nil {
		t.Fatalf("%v", err)
	}

	// Finish t1
	key1 := newKey(integHashTableName)
	t1item := make(map[string]*dynamodb.AttributeValue)
	for k, v := range key1 {
		t1item[k] = v
	}
	t1item["whoami"] = &dynamodb.AttributeValue{S: aws.String("t1")}
	t1PutInput := &dynamodb.PutItemInput{
		TableName: aws.String(integHashTableName),
		Item:      t1item,
	}
	if _, err := t1.PutItem(t1PutInput); err != nil {
		t.Fatalf("%v", err)
	}
	if err := assertItemLocked(assertItemLockedArg{tableName: integHashTableName, key: key1, owner: t1.ID(), isTransient: true, isApplied: true, checkTxItem: true}); err != nil {
		t.Fatalf("%v", err)
	}
	if err := t1.commit(); err != nil {
		t.Fatalf("%v", err)
	}
	if err := assertItemNotLocked(assertItemNotLockedArg{tableName: integHashTableName, key: key1, expected: t1item, shouldExist: true}); err != nil {
		t.Fatalf("%v", err)
	}

	// Begin t2
	t2item := make(map[string]*dynamodb.AttributeValue)
	for k, v := range key1 {
		t2item[k] = v
	}
	t2item["whoami"] = &dynamodb.AttributeValue{S: aws.String("t2")}
	t2item["t2stuff"] = &dynamodb.AttributeValue{S: aws.String("extra")}
	t2PutInput := &dynamodb.PutItemInput{
		TableName: aws.String(integHashTableName),
		Item:      t2item,
	}
	if _, err := t2.PutItem(t2PutInput); err != nil {
		t.Fatalf("%v", err)
	}
	if err := assertItemLocked(assertItemLockedArg{tableName: integHashTableName, key: key1, owner: t2.ID(), isTransient: false, isApplied: true, checkTxItem: true}); err != nil {
		t.Fatalf("%v", err)
	}

	// Begin and finish t3
	t3item := make(map[string]*dynamodb.AttributeValue)
	for k, v := range key1 {
		t3item[k] = v
	}
	t3item["whoami"] = &dynamodb.AttributeValue{S: aws.String("t3")}
	t3item["t3stuff"] = &dynamodb.AttributeValue{S: aws.String("things")}
	t3PutInput := &dynamodb.PutItemInput{
		TableName: aws.String(integHashTableName),
		Item:      t3item,
	}
	if _, err := t3.PutItem(t3PutInput); err != nil {
		t.Fatalf("%v", err)
	}
	if err := assertItemLocked(assertItemLockedArg{tableName: integHashTableName, key: key1, owner: t3.ID(), isTransient: false, isApplied: true, checkTxItem: true}); err != nil {
		t.Fatalf("%v", err)
	}
	if err := t3.commit(); err != nil {
		t.Fatalf("%v", err)
	}
	if err := assertItemNotLocked(assertItemNotLockedArg{tableName: integHashTableName, key: key1, expected: t3item, shouldExist: true}); err != nil {
		t.Fatalf("%v", err)
	}

	// Ensure t2 rolled back
	if err := t2.commit(); err == nil {
		t.Fatalf("t2 should fail")
	}

	if err := assertItemNotLocked(assertItemNotLockedArg{tableName: integHashTableName, key: key1, expected: t3item, shouldExist: true}); err != nil {
		t.Fatalf("%v", err)
	}
}

func TestFailValidationInApply(t *testing.T) {
	if err := setup(); err != nil {
		t.Fatalf("%v", err)
	}
	key := newKey(integHashTableName)
	t1, err := manager.newTransaction()
	if err != nil {
		t.Fatalf("%v", err)
	}
	t2, err := manager.newTransaction()
	if err != nil {
		t.Fatalf("%v", err)
	}

	eav := make(map[string]*dynamodb.AttributeValue)
	eav[":FooAttribute"] = &dynamodb.AttributeValue{S: aws.String("Bar")}
	t1UpdateInput := &dynamodb.UpdateItemInput{
		TableName: aws.String(integHashTableName),
		Key:       key,
		ExpressionAttributeValues: eav,
		UpdateExpression:          aws.String("SET FooAttribute = :FooAttribute"),
	}
	if _, err := t1.UpdateItem(t1UpdateInput); err != nil {
		t.Fatalf("%v", err)
	}
	if err := assertItemLocked(assertItemLockedArg{tableName: integHashTableName, key: key, owner: t1.ID(), isTransient: true, isApplied: true, checkTxItem: true}); err != nil {
		t.Fatalf("%v", err)
	}
	if err := t1.commit(); err != nil {
		t.Fatalf("%v", err)
	}
	if err := assertItemNotLocked(assertItemNotLockedArg{tableName: integHashTableName, key: key, shouldExist: true}); err != nil {
		t.Fatalf("%v", err)
	}

	eav2 := make(map[string]*dynamodb.AttributeValue)
	eav2[":FooAttributeIncr"] = &dynamodb.AttributeValue{N: aws.String("1")}
	t2UpdateInput := &dynamodb.UpdateItemInput{
		TableName: aws.String(integHashTableName),
		Key:       key,
		ExpressionAttributeValues: eav2,
		UpdateExpression:          aws.String("SET FooAttribute = FooAttribute + :FooAttributeIncr"),
	}
	_, err = t2.UpdateItem(t2UpdateInput)
	if err == nil {
		t.Fatalf("t2 should fail")
	}
	if !strings.Contains(err.Error(), "An operand in the update expression has an incorrect data type") {
		t.Fatalf("wrong error %v", err)
	}
	if err := assertItemLocked(assertItemLockedArg{tableName: integHashTableName, key: key, owner: t2.ID(), isTransient: false, isApplied: false, checkTxItem: true}); err != nil {
		t.Fatalf("%v", err)
	}

	if err := rollback(t2); err != nil {
		t.Fatalf("%v", err)
	}

	if err := assertItemNotLocked(assertItemNotLockedArg{tableName: integHashTableName, key: key, shouldExist: true}); err != nil {
		t.Fatalf("%v", err)
	}
}

func TestUseCommittedTransaction(t *testing.T) {
	if err := setup(); err != nil {
		t.Fatalf("%v", err)
	}

	keyID := randString(8)
	key1 := make(map[string]*dynamodb.AttributeValue)
	key1[idAttribute] = &dynamodb.AttributeValue{S: aws.String(fmt.Sprintf("val_%s", keyID))}
	deleteItem := func(tx *Transaction) error {
		key := make(map[string]*dynamodb.AttributeValue)
		key[idAttribute] = &dynamodb.AttributeValue{S: aws.String(fmt.Sprintf("val_%s", keyID))}
		input := &dynamodb.DeleteItemInput{
			TableName: aws.String(integHashTableName),
			Key:       key,
		}
		_, err := tx.DeleteItem(input)
		return err
	}

	tOK, err := manager.newTransaction()
	if err != nil {
		t.Fatalf("%v", err)
	}
	if err := deleteItem(tOK); err != nil {
		t.Fatalf("should succeed %v", err)
	}
	if err := tOK.commit(); err != nil {
		t.Fatalf("%v", err)
	}
	if err := assertItemNotLocked(assertItemNotLockedArg{tableName: integHashTableName, key: key1, shouldExist: false}); err != nil {
		t.Fatalf("%v", err)
	}

	t1, err := manager.newTransaction()
	if err != nil {
		t.Fatalf("%v", err)
	}
	if err := t1.commit(); err != nil {
		t.Fatalf("%v", err)
	}
	// Doing operations on a committed transaction should fail.
	if err := deleteItem(t1); err == nil {
		t.Fatalf("should fail")
	}
	if err := assertItemNotLocked(assertItemNotLockedArg{tableName: integHashTableName, key: key1, shouldExist: false}); err != nil {
		t.Fatalf("%v", err)
	}
}

func TestUseRolledBackTransaction(t *testing.T) {
	if err := setup(); err != nil {
		t.Fatalf("%v", err)
	}

	keyID := randString(8)
	key1 := make(map[string]*dynamodb.AttributeValue)
	key1[idAttribute] = &dynamodb.AttributeValue{S: aws.String(fmt.Sprintf("val_%s", keyID))}
	deleteItem := func(tx *Transaction) error {
		key := make(map[string]*dynamodb.AttributeValue)
		key[idAttribute] = &dynamodb.AttributeValue{S: aws.String(fmt.Sprintf("val_%s", keyID))}
		input := &dynamodb.DeleteItemInput{
			TableName: aws.String(integHashTableName),
			Key:       key,
		}
		_, err := tx.DeleteItem(input)
		return err
	}

	t1, err := manager.newTransaction()
	if err != nil {
		t.Fatalf("%v", err)
	}
	if err := rollback(t1); err != nil {
		t.Fatalf("%v", err)
	}
	if err := deleteItem(t1); err == nil {
		t.Fatalf("rolled back transaction should not be used")
	}
	t1ResumedTxItem, err := newTransactionItem(t1.ID(), manager, false)
	if err != nil {
		t.Fatalf("%v", err)
	}
	t1Resumed := &Transaction{
		txManager: manager,
		txItem:    t1ResumedTxItem,
		Retrier:   newDefaultJitterExpBackoff(),
	}
	if err := deleteItem(t1Resumed); err == nil {
		t.Fatalf("rolled back transaction should not be used")
	}

	t2, err := manager.newTransaction()
	if err != nil {
		t.Fatalf("%v", err)
	}
	if err := rollback(t2); err != nil {
		t.Fatalf("%v", err)
	}
	if err := t2.commit(); err == nil {
		t.Fatalf("rolled back transaction should not be committed")
	}
	t2ResumedTxItem, err := newTransactionItem(t2.ID(), manager, false)
	if err != nil {
		t.Fatalf("%v", err)
	}
	t2Resumed := &Transaction{
		txManager: manager,
		txItem:    t2ResumedTxItem,
		Retrier:   newDefaultJitterExpBackoff(),
	}
	if err := t2Resumed.commit(); err == nil {
		t.Fatalf("rolled back transaction should not be committed")
	}
}

func TestDriveCommit(t *testing.T) {
	if err := setup(); err != nil {
		t.Fatalf("%v", err)
	}
	t1, err := manager.newTransaction()
	if err != nil {
		t.Fatalf("%v", err)
	}
	key1 := newKey(integHashTableName)
	key2 := newKey(integHashTableName)
	item := make(map[string]*dynamodb.AttributeValue)
	for k, v := range key1 {
		item[k] = v
	}
	item["attr"] = &dynamodb.AttributeValue{S: aws.String("original")}
	putInput := &dynamodb.PutItemInput{
		TableName: aws.String(integHashTableName),
		Item:      item,
	}
	if _, err := t1.PutItem(putInput); err != nil {
		t.Fatalf("%v", err)
	}
	if err := t1.commit(); err != nil {
		t.Fatalf("%v", err)
	}
	if err := assertItemNotLocked(assertItemNotLockedArg{tableName: integHashTableName, key: key1, expected: item, shouldExist: true}); err != nil {
		t.Fatalf("%v", err)
	}
	if err := assertItemNotLocked(assertItemNotLockedArg{tableName: integHashTableName, key: key2, shouldExist: false}); err != nil {
		t.Fatalf("%v", err)
	}

	t2, err := manager.newTransaction()
	if err != nil {
		t.Fatalf("%v", err)
	}
	item["attr2"] = &dynamodb.AttributeValue{S: aws.String("new")}
	putInput2 := &dynamodb.PutItemInput{
		TableName: aws.String(integHashTableName),
		Item:      item,
	}
	if _, err := t2.PutItem(putInput2); err != nil {
		t.Fatalf("%v", err)
	}
	getInput2 := &dynamodb.GetItemInput{
		TableName: aws.String(integHashTableName),
		Key:       key2,
	}
	getResult2, err := t2.GetItem(getInput2)
	if err != nil {
		t.Fatalf("%v", err)
	}
	if getResult2.Item != nil {
		t.Fatalf("key2 %s should not exist", key2)
	}

	if err := assertItemLocked(assertItemLockedArg{tableName: integHashTableName, key: key1, expected: item, owner: t2.ID(), isTransient: false, isApplied: true, checkTxItem: true}); err != nil {
		t.Fatalf("%v", err)
	}
	if err := assertItemLocked(assertItemLockedArg{tableName: integHashTableName, key: key2, expected: key2, owner: t2.ID(), isTransient: true, isApplied: false, checkTxItem: true}); err != nil {
		t.Fatalf("%v", err)
	}

	if err := t2.commit(); err != nil {
		t.Fatalf("%v", err)
	}

	if err := assertItemNotLocked(assertItemNotLockedArg{tableName: integHashTableName, key: key1, expected: item, shouldExist: true}); err != nil {
		t.Fatalf("%v", err)
	}
	if err := assertItemNotLocked(assertItemNotLockedArg{tableName: integHashTableName, key: key2, shouldExist: false}); err != nil {
		t.Fatalf("%v", err)
	}
}

func TestDriveRollback(t *testing.T) {
	if err := setup(); err != nil {
		t.Fatalf("%v", err)
	}
	t1, err := manager.newTransaction()
	if err != nil {
		t.Fatalf("%v", err)
	}
	key1 := newKey(integHashTableName)
	item1 := make(map[string]*dynamodb.AttributeValue)
	for k, v := range key1 {
		item1[k] = v
	}
	item1["attr1"] = &dynamodb.AttributeValue{S: aws.String("original1")}
	key2 := newKey(integHashTableName)
	item2 := make(map[string]*dynamodb.AttributeValue)
	for k, v := range key2 {
		item2[k] = v
	}
	item2["attr2"] = &dynamodb.AttributeValue{S: aws.String("original2")}
	key3 := newKey(integHashTableName)

	putInput11 := &dynamodb.PutItemInput{
		TableName: aws.String(integHashTableName),
		Item:      item1,
	}
	if _, err := t1.PutItem(putInput11); err != nil {
		t.Fatalf("%v", err)
	}
	putInput12 := &dynamodb.PutItemInput{
		TableName: aws.String(integHashTableName),
		Item:      item2,
	}
	if _, err := t1.PutItem(putInput12); err != nil {
		t.Fatalf("%v", err)
	}
	if err := t1.commit(); err != nil {
		t.Fatalf("%v", err)
	}
	if err := assertItemNotLocked(assertItemNotLockedArg{tableName: integHashTableName, key: key1, expected: item1, shouldExist: true}); err != nil {
		t.Fatalf("%v", err)
	}
	if err := assertItemNotLocked(assertItemNotLockedArg{tableName: integHashTableName, key: key2, expected: item2, shouldExist: true}); err != nil {
		t.Fatalf("%v", err)
	}

	t2, err := manager.newTransaction()
	if err != nil {
		t.Fatalf("%v", err)
	}
	item1a := make(map[string]*dynamodb.AttributeValue)
	for k, v := range item1 {
		item1a[k] = v
	}
	item1a["attr1"] = &dynamodb.AttributeValue{S: aws.String("new1")}
	item1a["attr2"] = &dynamodb.AttributeValue{S: aws.String("new1")}
	putInput21 := &dynamodb.PutItemInput{
		TableName: aws.String(integHashTableName),
		Item:      item1a,
	}
	if _, err := t2.PutItem(putInput21); err != nil {
		t.Fatalf("%v", err)
	}
	getInput21 := &dynamodb.GetItemInput{
		TableName: aws.String(integHashTableName),
		Key:       key2,
	}
	getResult21, err := t2.GetItem(getInput21)
	if err != nil {
		t.Fatalf("%v", err)
	}
	if err := attributeValueMapEqual(getResult21.Item, item2); err != nil {
		t.Fatalf("attributeValueMapEqual %v", err)
	}
	getInput22 := &dynamodb.GetItemInput{
		TableName: aws.String(integHashTableName),
		Key:       key3,
	}
	getResult22, err := t2.GetItem(getInput22)
	if err != nil {
		t.Fatalf("%v", err)
	}
	if getResult22.Item != nil {
		t.Fatalf("key3 should not exists %+v", getResult22)
	}
	allItemsLocked := func() error {
		client := manager.client.(*failingAmazonDynamodbClient)
		oriFilter := client.getFilter
		client.getFilter = nil
		defer func() { client.getFilter = oriFilter }()

		if err := assertItemLocked(assertItemLockedArg{tableName: integHashTableName, key: key1, expected: item1a, owner: t2.ID(), isTransient: false, isApplied: true, checkTxItem: true}); err != nil {
			return fmt.Errorf("key1 %v", err)
		}
		if err := assertItemLocked(assertItemLockedArg{tableName: integHashTableName, key: key2, expected: item2, owner: t2.ID(), isTransient: false, isApplied: false, checkTxItem: true}); err != nil {
			return fmt.Errorf("key2 %v", err)
		}
		if err := assertItemLocked(assertItemLockedArg{tableName: integHashTableName, key: key3, expected: key3, owner: t2.ID(), isTransient: true, isApplied: false, checkTxItem: true}); err != nil {
			return fmt.Errorf("key3 %v", err)
		}
		return nil
	}
	if err := allItemsLocked(); err != nil {
		t.Fatalf("%v", err)
	}

	client := manager.client.(*failingAmazonDynamodbClient)
	client.getFilter = func(input *dynamodb.GetItemInput) (bool, *dynamodb.GetItemOutput, error) {
		if *input.TableName != integHashTableName {
			return false, nil, nil
		}
		return true, nil, fmt.Errorf("failed your request")
	}
	err = rollback(t2)
	if err == nil {
		t.Fatalf("t2 rollback should fail")
	}
	if !strings.Contains(err.Error(), "failed your request") {
		t.Fatalf("wrong error %v", err)
	}
	if err := allItemsLocked(); err != nil {
		t.Fatalf("%v", err)
	}

	client.getFilter = nil
	if err := rollback(t2); err != nil {
		t.Fatalf("%v", err)
	}

	if err := assertItemNotLocked(assertItemNotLockedArg{tableName: integHashTableName, key: key1, expected: item1, shouldExist: true}); err != nil {
		t.Fatalf("%v", err)
	}
	if err := assertItemNotLocked(assertItemNotLockedArg{tableName: integHashTableName, key: key2, expected: item2, shouldExist: true}); err != nil {
		t.Fatalf("%v", err)
	}
	if err := assertItemNotLocked(assertItemNotLockedArg{tableName: integHashTableName, key: key3, shouldExist: false}); err != nil {
		t.Fatalf("%v", err)
	}
}

func TestResumeRollbackAfterTransientApplyFailure(t *testing.T) {
	if err := setup(); err != nil {
		t.Fatalf("%v", err)
	}
	t1, err := manager.newTransaction()
	if err != nil {
		t.Fatalf("%v", err)
	}
	key1 := newKey(integHashTableName)
	item1 := make(map[string]*dynamodb.AttributeValue)
	for k, v := range key1 {
		item1[k] = v
	}
	item1["attr1"] = &dynamodb.AttributeValue{S: aws.String("original1")}
	putInput := &dynamodb.PutItemInput{
		TableName: aws.String(integHashTableName),
		Item:      item1,
	}

	client := manager.client.(*failingAmazonDynamodbClient)
	client.putFilter = func(input *dynamodb.PutItemInput) (bool, *dynamodb.PutItemOutput, error) {
		if *input.TableName != integHashTableName {
			return false, nil, nil
		}
		return true, nil, fmt.Errorf("failed your request")
	}
	if _, err := t1.PutItem(putInput); err == nil {
		t.Fatalf("item1 should fail")
	}
	client.putFilter = nil
	if err := assertItemLocked(assertItemLockedArg{tableName: integHashTableName, key: key1, expected: key1, owner: t1.ID(), isTransient: true, isApplied: false, checkTxItem: true}); err != nil {
		t.Fatalf("%v", err)
	}
	if err := assertOldItemImage(assertOldItemImageArg{txID: t1.ID(), tableName: integHashTableName, key: key1, expected: key1, shouldExist: false}); err != nil {
		t.Fatalf("%v", err)
	}

	t3TxItem, err := newTransactionItem(t1.ID(), manager, false)
	if err != nil {
		t.Fatalf("%v", err)
	}
	t3 := &Transaction{
		txManager: manager,
		txItem:    t3TxItem,
		Retrier:   newDefaultJitterExpBackoff(),
	}
	if err := rollback(t3); err != nil {
		t.Fatalf("%v", err)
	}

	if err := assertItemNotLocked(assertItemNotLockedArg{tableName: integHashTableName, key: key1, shouldExist: false}); err != nil {
		t.Fatalf("%v", err)
	}
	if err := assertOldItemImage(assertOldItemImageArg{txID: t1.ID(), tableName: integHashTableName, key: key1, shouldExist: false}); err != nil {
		t.Fatalf("%v", err)
	}
}

func TestUnlockInRollbackIfNoItemImageSaved(t *testing.T) {
	if err := setup(); err != nil {
		t.Fatalf("%v", err)
	}
	t1, err := manager.newTransaction()
	if err != nil {
		t.Fatalf("%v", err)
	}
	item0a := make(map[string]*dynamodb.AttributeValue)
	for k, v := range item0 {
		item0a[k] = v
	}
	item0a["attr1"] = &dynamodb.AttributeValue{S: aws.String("original1")}
	putInput1 := &dynamodb.PutItemInput{
		TableName: aws.String(integHashTableName),
		Item:      item0a,
	}

	// Change the existing item key0, failing when trying to save away the item image.
	client := manager.client.(*failingAmazonDynamodbClient)
	client.putFilter = func(input *dynamodb.PutItemInput) (bool, *dynamodb.PutItemOutput, error) {
		if *input.TableName != integImagesTableName {
			return false, nil, nil
		}
		return true, nil, fmt.Errorf("failed your request")
	}
	if _, err := t1.PutItem(putInput1); err == nil {
		t.Fatalf("put item0a should fail")
	}
	if err := assertItemLocked(assertItemLockedArg{tableName: integHashTableName, key: key0, expected: item0, owner: t1.ID(), isTransient: false, isApplied: false, checkTxItem: true}); err != nil {
		t.Fatalf("%v", err)
	}

	// Roll back, and ensure the item was reverted correctly.
	if err := rollback(t1); err != nil {
		t.Fatalf("%v", err)
	}
	if err := assertItemNotLocked(assertItemNotLockedArg{tableName: integHashTableName, key: key0, expected: item0, shouldExist: true}); err != nil {
		t.Fatalf("%v", err)
	}
}

func TestShouldNotCommitAfterRollback(t *testing.T) {
	for _, rollbackT1 := range []bool{true, false} {
		if err := setup(); err != nil {
			t.Fatalf("%v", err)
		}
		t1Client := &failingAmazonDynamodbClient{DynamoDB: dynamodb.New(sess)}
		t1Manager := NewTransactionManager(t1Client, integLockTableName, integImagesTableName, txTTLTimeout)
		t1, err := t1Manager.newTransaction()
		if err != nil {
			t.Fatalf("%v", err)
		}
		key1 := newKey(integHashTableName)
		item1 := make(map[string]*dynamodb.AttributeValue)
		for k, v := range key1 {
			item1[k] = v
		}
		item1["attr1"] = &dynamodb.AttributeValue{S: aws.String("original1")}
		putInput1 := &dynamodb.PutItemInput{
			TableName: aws.String(integHashTableName),
			Item:      item1,
		}

		// Block the locking of t1 until we provide a signal.
		goingToLock := make(chan struct{})
		startLock := make(chan struct{})
		t1Client.updateFilter = func(input *dynamodb.UpdateItemInput) (bool, *dynamodb.UpdateItemOutput, error) {
			if *input.TableName != integHashTableName {
				return false, nil, nil
			}
			goingToLock <- struct{}{}
			<-startLock
			return false, nil, nil
		}
		t1Err := make(chan error)
		go func() {
			_, err := t1.PutItem(putInput1)
			if err != nil {
				t1Err <- fmt.Errorf("PutItem %v", err)
				return
			}
			err = t1.commit()
			if err == nil {
				t1Err <- fmt.Errorf("t1 commit should fail since t2 rolled it back")
			} else {
				t1Err <- nil
			}
		}()

		<-goingToLock
		if err := assertItemNotLocked(assertItemNotLockedArg{tableName: integHashTableName, key: key1, shouldExist: false}); err != nil {
			t.Fatalf("%v", err)
		}
		txItem2, err := newTransactionItem(t1.ID(), manager, false)
		if err != nil {
			t.Fatalf("%v", err)
		}
		t2 := &Transaction{
			txManager: manager,
			txItem:    txItem2,
			Retrier:   newDefaultJitterExpBackoff(),
		}
		if err := rollback(t2); err != nil {
			t.Fatalf("%v", err)
		}
		if err := assertItemNotLocked(assertItemNotLockedArg{tableName: integHashTableName, key: key1, shouldExist: false}); err != nil {
			t.Fatalf("%v", err)
		}

		t1Client.updateFilter = nil
		close(startLock)
		err = <-t1Err
		if err != nil {
			t.Fatalf("%v", err)
		}
		if err := assertItemLocked(assertItemLockedArg{tableName: integHashTableName, key: key1, expected: item1, owner: t1.ID(), isTransient: true, isApplied: true, checkTxItem: true}); err != nil {
			t.Fatalf("%v", err)
		}

		if rollbackT1 {
			if err := rollback(t1); err != nil {
				t.Fatalf("%v", err)
			}
		}

		// Now start a new transaction involving key1 and make sure it will complete.
		t3, err := manager.newTransaction()
		if err != nil {
			t.Fatalf("%v", err)
		}
		item1a := make(map[string]*dynamodb.AttributeValue)
		for k, v := range key1 {
			item1a[k] = v
		}
		item1a["attr1"] = &dynamodb.AttributeValue{S: aws.String("new")}
		putInput3 := &dynamodb.PutItemInput{
			TableName: aws.String(integHashTableName),
			Item:      item1a,
		}
		if _, err := t3.PutItem(putInput3); err != nil {
			t.Fatalf("%v", err)
		}
		if err := assertItemLocked(assertItemLockedArg{tableName: integHashTableName, key: key1, expected: item1a, owner: t3.ID(), isTransient: true, isApplied: true, checkTxItem: true}); err != nil {
			t.Fatalf("%v", err)
		}
		if err := t3.commit(); err != nil {
			t.Fatalf("%v", err)
		}
		if err := assertItemNotLocked(assertItemNotLockedArg{tableName: integHashTableName, key: key1, expected: item1a, shouldExist: true}); err != nil {
			t.Fatalf("%v", err)
		}
	}
}

func TestRollbackAfterReadLockUpgradeAttempt(t *testing.T) {
	if err := setup(); err != nil {
		t.Fatalf("%v", err)
	}
	t1, err := manager.newTransaction()
	if err != nil {
		t.Fatalf("%v", err)
	}
	key1 := newKey(integHashTableName)
	getInput1 := &dynamodb.GetItemInput{
		TableName: aws.String(integHashTableName),
		Key:       key1,
	}
	getResult1, err := t1.GetItem(getInput1)
	if err != nil {
		t.Fatalf("%v", err)
	}
	if getResult1.Item != nil {
		t.Fatalf("item should not exist")
	}
	if err := assertItemLocked(assertItemLockedArg{tableName: integHashTableName, key: key1, owner: t1.ID(), isTransient: true, isApplied: false, checkTxItem: true}); err != nil {
		t.Fatalf("%v", err)
	}

	// Now start another transaction that is going to try to read that same item,
	// but stop after you read the competing transaction record (don't try to roll it back yet).
	t2Client := &failingAmazonDynamodbClient{DynamoDB: dynamodb.New(sess)}
	t2Manager := NewTransactionManager(t2Client, integLockTableName, integImagesTableName, txTTLTimeout)
	t2, err := t2Manager.newTransaction()
	if err != nil {
		t.Fatalf("%v", err)
	}
	goingToRollbackT1 := make(chan struct{})
	rollbackT1 := make(chan struct{})
	t2Client.updateFilter = func(input *dynamodb.UpdateItemInput) (bool, *dynamodb.UpdateItemOutput, error) {
		if *input.TableName != integLockTableName {
			return false, nil, nil
		}
		txID := *input.Key[AttributeNameTxID].S
		if txID != t1.ID() {
			return false, nil, nil
		}

		goingToRollbackT1 <- struct{}{}
		// t1 will be modified in the meantime.
		<-rollbackT1
		return false, nil, nil
	}
	t2Err := make(chan error)
	go func() {
		input := &dynamodb.GetItemInput{
			TableName: aws.String(integHashTableName),
			Key:       key1,
		}
		res, err := t2.GetItem(input)
		if err != nil {
			t2Err <- err
			return
		}
		if res.Item != nil {
			t2Err <- fmt.Errorf("should get nothing %+v", res)
			return
		}
		t2Err <- nil
	}()

	// Wait for t2 to get to the point where it loaded the t1 tx record.
	<-goingToRollbackT1
	// Now change that getItem to an updateItem in t1
	updateInput := &dynamodb.UpdateItemInput{
		TableName: aws.String(integHashTableName),
		Key:       key1,
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{":asdf": &dynamodb.AttributeValue{S: aws.String("wef")}},
		UpdateExpression:          aws.String("SET asdf = :asdf"),
	}
	if _, err := t1.UpdateItem(updateInput); err != nil {
		t.Fatalf("%v", err)
	}
	// Now let t2 continue on and roll back t1
	t2Client.updateFilter = nil
	rollbackT1 <- struct{}{}

	// Wait for t2 to finish rolling back t1
	err = <-t2Err
	if err != nil {
		t.Fatalf("%v", err)
	}

	key2 := newKey(integHashTableName)
	getInput2 := &dynamodb.GetItemInput{
		TableName: aws.String(integHashTableName),
		Key:       key2,
	}
	if _, err := t1.GetItem(getInput2); err == nil {
		t.Fatalf("t1 should be rolled back and unable to do stuff")
	}
	t1State, err := t1.txItem.getState()
	if err != nil {
		t.Fatalf("%v", err)
	}
	if t1State != TransactionItemStateRolledBack {
		t.Fatalf("t1 should be rolled back %s", t1State)
	}
}

func TestCommitCleanupFailedUnlockItemAfterCommit(t *testing.T) {
	if err := setup(); err != nil {
		t.Fatalf("%v", err)
	}
	t1Client := &failingAmazonDynamodbClient{DynamoDB: dynamodb.New(sess)}
	t1Manager := NewTransactionManager(t1Client, integLockTableName, integImagesTableName, txTTLTimeout)
	t1, err := t1Manager.newTransaction()
	if err != nil {
		t.Fatalf("%v", err)
	}
	key1 := newKey(integHashTableName)
	item1 := make(map[string]*dynamodb.AttributeValue)
	for k, v := range key1 {
		item1[k] = v
	}
	item1["attr_s"] = &dynamodb.AttributeValue{S: aws.String("wef")}
	putInput1 := &dynamodb.PutItemInput{
		TableName: aws.String(integHashTableName),
		Item:      item1,
	}
	if _, err := t1.PutItem(putInput1); err != nil {
		t.Fatalf("%v", err)
	}

	// Fail the unlockItemAfterCommit
	t1Client.updateFilter = func(input *dynamodb.UpdateItemInput) (bool, *dynamodb.UpdateItemOutput, error) {
		if *input.TableName != integHashTableName {
			return false, nil, nil
		}
		return true, nil, fmt.Errorf("failed your request")
	}
	if err := t1.commit(); err != nil {
		t.Fatalf("%v", err)
	}
	if err := assertItemLocked(assertItemLockedArg{tableName: integHashTableName, key: key1, expected: item1, owner: t1.ID(), isTransient: true, isApplied: true, checkTxItem: true}); err != nil {
		t.Fatalf("%v", err)
	}

	// The next commit comes and cleanup the previous unlockItemAfterCommit failure.
	t2, err := manager.newTransaction()
	if err != nil {
		t.Fatalf("%v", err)
	}
	item2 := make(map[string]*dynamodb.AttributeValue)
	for k, v := range key1 {
		item2[k] = v
	}
	item2["attr_s"] = &dynamodb.AttributeValue{S: aws.String("wef2")}
	putInput2 := &dynamodb.PutItemInput{
		TableName: aws.String(integHashTableName),
		Item:      item2,
	}
	if _, err := t2.PutItem(putInput2); err != nil {
		t.Fatalf("%v", err)
	}
	if err := t2.commit(); err != nil {
		t.Fatalf("%v", err)
	}
	if err := assertItemNotLocked(assertItemNotLockedArg{tableName: integHashTableName, key: key1, expected: item2, shouldExist: true}); err != nil {
		t.Fatalf("%v", err)
	}
}

func TestRollbackCleanupFailedUnlockItemAfterCommit(t *testing.T) {
	if err := setup(); err != nil {
		t.Fatalf("%v", err)
	}
	t1Client := &failingAmazonDynamodbClient{DynamoDB: dynamodb.New(sess)}
	t1Manager := NewTransactionManager(t1Client, integLockTableName, integImagesTableName, txTTLTimeout)
	t1, err := t1Manager.newTransaction()
	if err != nil {
		t.Fatalf("%v", err)
	}
	key1 := newKey(integHashTableName)
	item1 := make(map[string]*dynamodb.AttributeValue)
	for k, v := range key1 {
		item1[k] = v
	}
	item1["attr_s"] = &dynamodb.AttributeValue{S: aws.String("wef")}
	putInput1 := &dynamodb.PutItemInput{
		TableName: aws.String(integHashTableName),
		Item:      item1,
	}
	if _, err := t1.PutItem(putInput1); err != nil {
		t.Fatalf("%v", err)
	}

	// Fail the unlockItemAfterCommit
	t1Client.updateFilter = func(input *dynamodb.UpdateItemInput) (bool, *dynamodb.UpdateItemOutput, error) {
		if *input.TableName != integHashTableName {
			return false, nil, nil
		}
		return true, nil, fmt.Errorf("failed your request")
	}
	if err := t1.commit(); err != nil {
		t.Fatalf("%v", err)
	}
	if err := assertItemLocked(assertItemLockedArg{tableName: integHashTableName, key: key1, expected: item1, owner: t1.ID(), isTransient: true, isApplied: true, checkTxItem: true}); err != nil {
		t.Fatalf("%v", err)
	}

	// The next rollback comes and cleanup the previous unlockItemAfterCommit failure.
	txItem, err := newTransactionItem(t1.ID(), manager, false)
	if err != nil {
		t.Fatalf("%v", err)
	}
	if err := manager.Sweep(txItem.txItem, 0, true); err != nil {
		t.Fatalf("%v", err)
	}
	if err := assertItemNotLocked(assertItemNotLockedArg{tableName: integHashTableName, key: key1, expected: item1, shouldExist: true}); err != nil {
		t.Fatalf("%v", err)
	}
}

func TestBasicNewItemRollback(t *testing.T) {
	if err := setup(); err != nil {
		t.Fatalf("%v", err)
	}
	t1, err := manager.newTransaction()
	if err != nil {
		t.Fatalf("%v", err)
	}
	key1 := newKey(integHashTableName)

	updateInput := &dynamodb.UpdateItemInput{
		TableName: aws.String(integHashTableName),
		Key:       key1,
	}
	if _, err := t1.UpdateItem(updateInput); err != nil {
		t.Fatalf("%v", err)
	}
	if err := assertItemLocked(assertItemLockedArg{tableName: integHashTableName, key: key1, owner: t1.ID(), isTransient: true, isApplied: true, checkTxItem: true}); err != nil {
		t.Fatalf("%v", err)
	}

	if err := rollback(t1); err != nil {
		t.Fatalf("%v", err)
	}
	if err := assertItemNotLocked(assertItemNotLockedArg{tableName: integHashTableName, key: key1, shouldExist: false}); err != nil {
		t.Fatalf("%v", err)
	}
}

func TestBasicNewItemCommit(t *testing.T) {
	if err := setup(); err != nil {
		t.Fatalf("%v", err)
	}
	t1, err := manager.newTransaction()
	if err != nil {
		t.Fatalf("%v", err)
	}
	key1 := newKey(integHashTableName)

	updateInput := &dynamodb.UpdateItemInput{
		TableName: aws.String(integHashTableName),
		Key:       key1,
	}
	if _, err := t1.UpdateItem(updateInput); err != nil {
		t.Fatalf("%v", err)
	}
	if err := assertItemLocked(assertItemLockedArg{tableName: integHashTableName, key: key1, owner: t1.ID(), isTransient: true, isApplied: true, checkTxItem: true}); err != nil {
		t.Fatalf("%v", err)
	}

	if err := t1.commit(); err != nil {
		t.Fatalf("%v", err)
	}
	if err := assertItemNotLocked(assertItemNotLockedArg{tableName: integHashTableName, key: key1, expected: key1, shouldExist: true}); err != nil {
		t.Fatalf("%v", err)
	}
}

func TestMissingTableName(t *testing.T) {
	if err := setup(); err != nil {
		t.Fatalf("%v", err)
	}
	t1, err := manager.newTransaction()
	if err != nil {
		t.Fatalf("%v", err)
	}
	key1 := newKey(integHashTableName)

	updateInput := &dynamodb.UpdateItemInput{
		Key: key1,
	}
	_, err = t1.UpdateItem(updateInput)
	if err == nil {
		t.Fatalf("should fail without TableName")
	}
	if !strings.Contains(err.Error(), "tableName must not be null") {
		t.Fatalf("wrong error %v", err)
	}

	if err := assertItemNotLocked(assertItemNotLockedArg{tableName: integHashTableName, key: key1, shouldExist: false}); err != nil {
		t.Fatalf("%v", err)
	}
	if err := rollback(t1); err != nil {
		t.Fatalf("%v", err)
	}
	if err := assertItemNotLocked(assertItemNotLockedArg{tableName: integHashTableName, key: key1, shouldExist: false}); err != nil {
		t.Fatalf("%v", err)
	}
}

func TestEmptyTransaction(t *testing.T) {
	if err := setup(); err != nil {
		t.Fatalf("%v", err)
	}
	t1, err := manager.newTransaction()
	if err != nil {
		t.Fatalf("%v", err)
	}
	if err := t1.commit(); err != nil {
		t.Fatalf("%v", err)
	}
	if err := manager.Sweep(t1.txItem.txItem, 0, true); err != nil {
		t.Fatalf("%v", err)
	}
	if err := assertTransactionDeleted(t1); err != nil {
		t.Fatalf("%v", err)
	}
}

func TestMissingKey(t *testing.T) {
	if err := setup(); err != nil {
		t.Fatalf("%v", err)
	}
	t1, err := manager.newTransaction()
	if err != nil {
		t.Fatalf("%v", err)
	}
	updateInput := &dynamodb.UpdateItemInput{
		TableName: aws.String(integHashTableName),
	}
	_, err = t1.UpdateItem(updateInput)
	if err == nil {
		t.Fatalf("update should fail")
	}
	if !strings.Contains(err.Error(), "the request key cannot be empty") {
		t.Fatalf("wrong error %v", err)
	}
	if err := rollback(t1); err != nil {
		t.Fatalf("%v", err)
	}
}

func TestTooMuchDataInTransaction(t *testing.T) {
	if err := setup(); err != nil {
		t.Fatalf("%v", err)
	}

	bigStringSize := float64(maxItemSizeBytes) / 1.5
	bigBytes := make([]byte, int(bigStringSize))
	for i := 0; i < len(bigBytes); i++ {
		bigBytes[i] = 'a'
	}
	bigString := string(bigBytes)

	t1, err := manager.newTransaction()
	if err != nil {
		t.Fatalf("%v", err)
	}
	t2, err := manager.newTransaction()
	if err != nil {
		t.Fatalf("%v", err)
	}
	key1 := newKey(integHashTableName)
	item1 := make(map[string]*dynamodb.AttributeValue)
	for k, v := range key1 {
		item1[k] = v
	}
	item1["bigattr"] = &dynamodb.AttributeValue{S: aws.String("little")}
	putInput1 := &dynamodb.PutItemInput{
		TableName: aws.String(integHashTableName),
		Item:      item1,
	}
	if _, err := t1.PutItem(putInput1); err != nil {
		t.Fatalf("%v", err)
	}
	if err := assertItemLocked(assertItemLockedArg{tableName: integHashTableName, key: key1, expected: item1, owner: t1.ID(), isTransient: true, isApplied: true, checkTxItem: true}); err != nil {
		t.Fatalf("%v", err)
	}
	if err := t1.commit(); err != nil {
		t.Fatalf("%v", err)
	}
	if err := assertItemNotLocked(assertItemNotLockedArg{tableName: integHashTableName, key: key1, expected: item1, shouldExist: true}); err != nil {
		t.Fatalf("%v", err)
	}

	item1a := make(map[string]*dynamodb.AttributeValue)
	for k, v := range key1 {
		item1a[k] = v
	}
	item1a["bigattr"] = &dynamodb.AttributeValue{S: aws.String(bigString)}
	putInput1a := &dynamodb.PutItemInput{
		TableName: aws.String(integHashTableName),
		Item:      item1a,
	}
	if _, err := t2.PutItem(putInput1a); err != nil {
		t.Fatalf("%v", err)
	}
	if err := assertItemLocked(assertItemLockedArg{tableName: integHashTableName, key: key1, expected: item1a, owner: t2.ID(), isTransient: false, isApplied: true, checkTxItem: true}); err != nil {
		t.Fatalf("%v", err)
	}

	key2 := newKey(integHashTableName)
	item2 := make(map[string]*dynamodb.AttributeValue)
	for k, v := range key2 {
		item2[k] = v
	}
	item2["bigattr"] = &dynamodb.AttributeValue{S: aws.String(bigString)}
	putInput2 := &dynamodb.PutItemInput{
		TableName: aws.String(integHashTableName),
		Item:      item2,
	}
	_, err = t2.PutItem(putInput2)
	if err == nil {
		t.Fatalf("putInput2 should fail")
	}
	if !strings.Contains(err.Error(), "Item size to update has exceeded the maximum allowed size") {
		t.Fatalf("wrong error %v", err)
	}
	if err := assertItemNotLocked(assertItemNotLockedArg{tableName: integHashTableName, key: key2, shouldExist: false}); err != nil {
		t.Fatalf("%v", err)
	}
	if err := assertItemLocked(assertItemLockedArg{tableName: integHashTableName, key: key1, expected: item1a, owner: t2.ID(), isTransient: false, isApplied: true, checkTxItem: true}); err != nil {
		t.Fatalf("%v", err)
	}

	if err := rollback(t2); err != nil {
		t.Fatalf("%v", err)
	}
	if err := assertItemNotLocked(assertItemNotLockedArg{tableName: integHashTableName, key: key1, expected: item1, shouldExist: true}); err != nil {
		t.Fatalf("%v", err)
	}
	if err := assertItemNotLocked(assertItemNotLockedArg{tableName: integHashTableName, key: key2, shouldExist: false}); err != nil {
		t.Fatalf("%v", err)
	}
}

func TestContainsBinaryAttributes(t *testing.T) {
	if err := setup(); err != nil {
		t.Fatalf("%v", err)
	}
	t1, err := manager.newTransaction()
	if err != nil {
		t.Fatalf("%v", err)
	}
	key := newKey(integHashTableName)
	item := make(map[string]*dynamodb.AttributeValue)
	for k, v := range key {
		item[k] = v
	}
	item["attr_b"] = &dynamodb.AttributeValue{B: []byte("asdf\n\t\u0123")}
	item["attr_bs"] = &dynamodb.AttributeValue{BS: [][]byte{[]byte("asdf\n\t\u0123"), []byte("wef")}}

	input := &dynamodb.PutItemInput{
		TableName: aws.String(integHashTableName),
		Item:      item,
	}
	if _, err := t1.PutItem(input); err != nil {
		t.Fatalf("%v", err)
	}
	if err := assertItemLocked(assertItemLockedArg{tableName: integHashTableName, key: key, expected: item, owner: t1.ID(), isTransient: true, isApplied: true, checkTxItem: true}); err != nil {
		t.Fatalf("%v", err)
	}

	if err := t1.commit(); err != nil {
		t.Fatalf("%v", err)
	}
	if err := assertItemNotLocked(assertItemNotLockedArg{tableName: integHashTableName, key: key, expected: item, shouldExist: true}); err != nil {
		t.Fatalf("%v", err)
	}
}

func TestContainsJSONAttributes(t *testing.T) {
	if err := setup(); err != nil {
		t.Fatalf("%v", err)
	}
	t1, err := manager.newTransaction()
	if err != nil {
		t.Fatalf("%v", err)
	}
	key := newKey(integHashTableName)
	item := make(map[string]*dynamodb.AttributeValue)
	for k, v := range key {
		item[k] = v
	}
	item["attr_json"] = &dynamodb.AttributeValue{M: jsonMAttrVal}

	input := &dynamodb.PutItemInput{
		TableName: aws.String(integHashTableName),
		Item:      item,
	}
	if _, err := t1.PutItem(input); err != nil {
		t.Fatalf("%v", err)
	}
	if err := assertItemLocked(assertItemLockedArg{tableName: integHashTableName, key: key, expected: item, owner: t1.ID(), isTransient: true, isApplied: true, checkTxItem: true}); err != nil {
		t.Fatalf("%v", err)
	}

	if err := t1.commit(); err != nil {
		t.Fatalf("%v", err)
	}
	if err := assertItemNotLocked(assertItemNotLockedArg{tableName: integHashTableName, key: key, expected: item, shouldExist: true}); err != nil {
		t.Fatalf("%v", err)
	}
}

func TestContainsSpecialAttributes(t *testing.T) {
	if err := setup(); err != nil {
		t.Fatalf("%v", err)
	}

	for _, attr := range []string{AttributeNameTxID, AttributeNameTransient, AttributeNameApplied} {
		t1, err := manager.newTransaction()
		if err != nil {
			t.Fatalf("%v", err)
		}
		key := newKey(integHashTableName)
		item := make(map[string]*dynamodb.AttributeValue)
		for k, v := range key {
			item[k] = v
		}
		item[attr] = &dynamodb.AttributeValue{S: aws.String("asdf")}

		input := &dynamodb.PutItemInput{
			TableName: aws.String(integHashTableName),
			Item:      item,
		}
		_, err = t1.PutItem(input)
		if err == nil {
			t.Fatalf("should fail")
		}
		if !strings.Contains(err.Error(), "must not contain the reserved") {
			t.Fatalf("wrong error %v", err)
		}
	}
}

func TestOneTransactionPerItem(t *testing.T) {
	if err := setup(); err != nil {
		t.Fatalf("%v", err)
	}
	transaction, err := manager.newTransaction()
	if err != nil {
		t.Fatalf("%v", err)
	}
	key := newKey(integHashTableName)

	putInput1 := &dynamodb.PutItemInput{
		TableName: aws.String(integHashTableName),
		Item:      key,
	}
	if _, err := transaction.PutItem(putInput1); err != nil {
		t.Fatalf("%v", err)
	}

	putInput2 := &dynamodb.PutItemInput{
		TableName: aws.String(integHashTableName),
		Item:      key,
	}
	_, err = transaction.PutItem(putInput2)
	if err == nil {
		t.Fatalf("should fail")
	}
	if _, ok := errors.Cause(err).(*DuplicateRequestError); !ok {
		t.Fatalf("wrong error %v", err)
	}

	if err := rollback(transaction); err != nil {
		t.Fatalf("%v", err)
	}
	if err := assertItemNotLocked(assertItemNotLockedArg{tableName: integHashTableName, key: key, shouldExist: false}); err != nil {
		t.Fatalf("%v", err)
	}
}

func TestCommitCleansUpImages(t *testing.T) {
	t1, err := manager.newTransaction()
	if err != nil {
		t.Fatalf("%+v", err)
	}
	item := make(map[string]*dynamodb.AttributeValue)
	for k, v := range item0 {
		item[k] = v
	}
	item["newAttr"] = &dynamodb.AttributeValue{S: aws.String("abc")}
	putInput := &dynamodb.PutItemInput{
		TableName: aws.String(integHashTableName),
		Item:      item,
	}
	if _, err := t1.PutItem(putInput); err != nil {
		t.Fatalf("%+v", err)
	}

	if err := assertImagesTableEmpty(); err == nil {
		t.Fatalf("images table should not be empty")
	}
	if err := t1.commit(); err != nil {
		t.Fatalf("%v", err)
	}
	if err := assertImagesTableEmpty(); err != nil {
		t.Fatalf("%v", err)
	}
}

func TestSweepCleansUpImages(t *testing.T) {
	t1, err := manager.newTransaction()
	if err != nil {
		t.Fatalf("%+v", err)
	}
	item := make(map[string]*dynamodb.AttributeValue)
	for k, v := range item0 {
		item[k] = v
	}
	item["newAttr"] = &dynamodb.AttributeValue{S: aws.String("abc")}
	putInput := &dynamodb.PutItemInput{
		TableName: aws.String(integHashTableName),
		Item:      item,
	}
	if _, err := t1.PutItem(putInput); err != nil {
		t.Fatalf("%+v", err)
	}

	if err := assertImagesTableEmpty(); err == nil {
		t.Fatalf("images table should not be empty")
	}
	txItemReloaded, err := newTransactionItem(t1.ID(), manager, false)
	if err != nil {
		t.Fatalf("%v", err)
	}
	if err := manager.Sweep(txItemReloaded.txItem, 0, true); err != nil {
		t.Fatalf("%v", err)
	}
	if err := assertImagesTableEmpty(); err != nil {
		t.Fatalf("%v", err)
	}
}

type assertOldItemImageArg struct {
	txID        string
	tableName   string
	key         map[string]*dynamodb.AttributeValue
	expected    map[string]*dynamodb.AttributeValue
	shouldExist bool
}

func assertOldItemImage(arg assertOldItemImageArg) error {
	txItem, err := newTransactionItem(arg.txID, manager, false)
	if err != nil {
		return fmt.Errorf("newTransactionItem %v", err)
	}
	immutableKey, err := toImmutableKey(arg.key)
	if err != nil {
		return fmt.Errorf("toImmutableKey %v", err)
	}
	r := txItem.requestsMap[arg.tableName][immutableKey]
	image, err := txItem.loadItemImage(r.getRid())
	if err != nil {
		return fmt.Errorf("loadItemImage %v", err)
	}

	if arg.shouldExist {
		if image == nil {
			return fmt.Errorf("image not exist")
		}
		delete(image, AttributeNameTxID)
		delete(image, AttributeNameImageID)
		delete(image, AttributeNameDate)
		if _, ok := image[AttributeNameTransient]; ok {
			return fmt.Errorf("image should not contain transient %+v", image)
		}
		if err := attributeValueMapEqual(image, arg.expected); err != nil {
			return fmt.Errorf("attributeValueMapEqual %v", err)
		}
	} else {
		if image != nil {
			return fmt.Errorf("image exists %+v", image)
		}
	}

	return nil
}

type assertItemNotLockedArg struct {
	tableName   string
	key         map[string]*dynamodb.AttributeValue
	expected    map[string]*dynamodb.AttributeValue
	shouldExist bool
}

func assertItemNotLocked(arg assertItemNotLockedArg) error {
	item, err := getItemByKey(manager, arg.tableName, arg.key)
	if err != nil {
		if _, ok := err.(*notFoundError); ok && !arg.shouldExist {
			return nil
		}
		return fmt.Errorf("getItemByKey %v", err)
	}
	if _, ok := item[AttributeNameTransient]; ok {
		return fmt.Errorf("item is transient %+v", item)
	}
	if _, ok := item[AttributeNameTxID]; ok {
		return fmt.Errorf("item is locked %+v", item)
	}
	if _, ok := item[AttributeNameApplied]; ok {
		return fmt.Errorf("item is applied %+v", item)
	}
	if _, ok := item[AttributeNameDate]; ok {
		return fmt.Errorf("item has date %+v", item)
	}

	if arg.expected != nil {
		delete(item, AttributeNameTxID)
		delete(item, AttributeNameTransient)
		if err := attributeValueMapEqual(arg.expected, item); err != nil {
			return fmt.Errorf("attributeValueMapEqual %v", err)
		}
	}
	return nil
}

type assertItemLockedArg struct {
	tableName   string
	key         map[string]*dynamodb.AttributeValue
	expected    map[string]*dynamodb.AttributeValue
	owner       string
	isTransient bool
	isApplied   bool
	checkTxItem bool
}

func assertItemLocked(arg assertItemLockedArg) error {
	item, err := getItemByKey(manager, arg.tableName, arg.key)
	if err != nil {
		return fmt.Errorf("getItemByKey %v", err)
	}
	if item == nil {
		return fmt.Errorf("item not found %+v %v", arg, err)
	}
	txID, ok := item[AttributeNameTxID]
	if !ok || txID.S == nil {
		return fmt.Errorf("item not in transaction %+v", item)
	}
	if *txID.S != arg.owner {
		return fmt.Errorf("wrong transaction %s %s", *txID.S, arg.owner)
	}

	if arg.isTransient {
		v, ok := item[AttributeNameTransient]
		if !ok || v.S == nil {
			return fmt.Errorf("item not transient %+v", item)
		}
		if *v.S != "1" {
			return fmt.Errorf("wrong transient value %+v", item)
		}
	} else {
		if _, ok := item[AttributeNameTransient]; ok {
			return fmt.Errorf("item is transient %v", item)
		}
	}

	if arg.isApplied {
		v, ok := item[AttributeNameApplied]
		if !ok || v.S == nil {
			return fmt.Errorf("item not applied %+v", item)
		}
		if *v.S != "1" {
			return fmt.Errorf("wrong applied value %+v", item)
		}
	} else {
		if _, ok := item[AttributeNameApplied]; ok {
			return fmt.Errorf("item is applied %v", item)
		}
	}

	if _, ok := item[AttributeNameDate]; !ok {
		return fmt.Errorf("item not date %+v", item)
	}

	if arg.expected != nil {
		delete(item, AttributeNameTxID)
		delete(item, AttributeNameTransient)
		delete(item, AttributeNameApplied)
		delete(item, AttributeNameDate)
		if err := attributeValueMapEqual(arg.expected, item); err != nil {
			return fmt.Errorf("attributeValueMapEqual %v", err)
		}
	}

	if arg.checkTxItem {
		insert := false
		txItem, err := newTransactionItem(arg.owner, manager, insert)
		if err != nil {
			return fmt.Errorf("newTransactionItem %v", err)
		}
		tbReqs, ok := txItem.requestsMap[arg.tableName]
		if !ok {
			return fmt.Errorf("no request for table %s %+v", arg.tableName, txItem)
		}
		immutableKey, err := toImmutableKey(arg.key)
		if err != nil {
			return fmt.Errorf("toImmutableKey %v", err)
		}
		if _, ok := tbReqs[immutableKey]; !ok {
			return fmt.Errorf("no request for immutable key %s", immutableKey)
		}
	}
	return nil
}

func attributeValueMapEqual(a, b map[string]*dynamodb.AttributeValue) error {
	if len(a) != len(b) {
		return fmt.Errorf("different size %d %d", len(a), len(b))
	}
	for k, v := range a {
		bv, ok := b[k]
		if !ok {
			return fmt.Errorf("b does not have key %s", k)
		}
		if v.NS != nil {
			if err := dynamodbSetsEqual(v.NS, bv.NS); err != nil {
				return fmt.Errorf("dynamodbSetsEqual NS %v", err)
			}
		} else if v.SS != nil {
			if err := dynamodbSetsEqual(v.SS, bv.SS); err != nil {
				return fmt.Errorf("dynamodbSetsEqual SS %v", err)
			}
		} else if v.M != nil {
			if err := attributeValueMapEqual(v.M, bv.M); err != nil {
				return fmt.Errorf("attributeValueMapEqual %s %v", k, err)
			}
		} else {
			if v.String() != bv.String() {
				return fmt.Errorf("different values %s %s", v.String(), bv.String())
			}
		}
	}
	return nil
}

func dynamodbSetsEqual(a, b []*string) error {
	aMap := make(map[string]struct{})
	for _, e := range a {
		aMap[*e] = struct{}{}
	}
	bMap := make(map[string]struct{})
	for _, e := range b {
		bMap[*e] = struct{}{}
	}

	if len(aMap) != len(bMap) {
		return fmt.Errorf("different sizes %d %d", len(aMap), len(bMap))
	}
	for k, _ := range aMap {
		if _, ok := bMap[k]; !ok {
			return fmt.Errorf("no key in b %s", k)
		}
	}
	return nil
}

func assertTransactionDeleted(tx *Transaction) error {
	input := &dynamodb.GetItemInput{
		TableName:      aws.String(tx.txManager.transactionTableName),
		Key:            tx.txItem.txKey,
		ConsistentRead: aws.Bool(true),
	}
	resp, err := tx.txManager.client.GetItem(input)
	if err != nil {
		return fmt.Errorf("GetItem %v", err)
	}
	if _, err := ttlFromItem(resp.Item); err != nil {
		return errors.Wrap(err, fmt.Sprintf("ttlFromItem %+v", resp))
	}
	return nil
}

func assertImagesTableEmpty() error {
	client := dynamodb.New(sess)
	input := &dynamodb.ScanInput{
		TableName: aws.String(integImagesTableName),
	}
	output, err := client.Scan(input)
	if err != nil {
		return errors.Wrap(err, "Scan")
	}
	for _, item := range output.Items {
		if _, err := ttlFromItem(item); err != nil {
			return errors.Wrap(err, "ttlFromItem")
		}
	}
	return nil
}

func ttlFromItem(item map[string]*dynamodb.AttributeValue) (int, error) {
	ttlAV, ok := item[AttributeNameTTL]
	if !ok {
		return -1, fmt.Errorf("no AttributeNameTTL %+v", item)
	}
	if ttlAV.N == nil {
		return -1, fmt.Errorf("AttributeNameTTL is nil %+v", item)
	}
	ttl, err := strconv.Atoi(*ttlAV.N)
	if err != nil {
		return -1, errors.Wrap(err, "strconv.Atoi")
	}
	return ttl, nil
}

func newKey(tableName string) map[string]*dynamodb.AttributeValue {
	if tableName == integHashTableName {
		key := make(map[string]*dynamodb.AttributeValue)
		key[idAttribute] = &dynamodb.AttributeValue{S: aws.String(fmt.Sprintf("val_%s", randString(8)))}
		return key
	}
	return nil
}
