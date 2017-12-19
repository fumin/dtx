package dtx

import (
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/pkg/errors"
)

var (
	transactionsTableAttributes = []*dynamodb.AttributeDefinition{
		&dynamodb.AttributeDefinition{
			AttributeName: aws.String(AttributeNameTxID),
			AttributeType: aws.String(dynamodb.ScalarAttributeTypeS),
		},
	}

	transactionsTableKeySchema = []*dynamodb.KeySchemaElement{
		&dynamodb.KeySchemaElement{
			AttributeName: aws.String(AttributeNameTxID),
			KeyType:       aws.String(dynamodb.KeyTypeHash),
		},
	}

	transactionsImagesTableAttributes = []*dynamodb.AttributeDefinition{
		&dynamodb.AttributeDefinition{
			AttributeName: aws.String(AttributeNameImageID),
			AttributeType: aws.String(dynamodb.ScalarAttributeTypeS),
		},
	}

	transactionsImagesTableKeySchema = []*dynamodb.KeySchemaElement{
		&dynamodb.KeySchemaElement{
			AttributeName: aws.String(AttributeNameImageID),
			KeyType:       aws.String(dynamodb.KeyTypeHash),
		},
	}
)

// A TransactionManager creates new transactions and assists them during their lifetime.
// TransactionManagers are safe for concurrent use by multiple goroutines.
type TransactionManager struct {
	mutex sync.RWMutex

	client               dynamodbiface.DynamoDBAPI
	transactionTableName string
	itemImageTableName   string
	ttlTimeout           time.Duration
	tableSchemaCache     map[string][]*dynamodb.KeySchemaElement
}

// NewTransactionManager creates a TransactionManager, given a DynamoDB client,
// the names of the transaction and transaction item image tables,
// and the Time To Live timeout when deleting transactions.
func NewTransactionManager(client dynamodbiface.DynamoDBAPI, transactionTableName string, itemImageTableName string, ttlTimeout time.Duration) *TransactionManager {
	mg := TransactionManager{
		client:               client,
		transactionTableName: transactionTableName,
		itemImageTableName:   itemImageTableName,
		ttlTimeout:           ttlTimeout,
	}
	mg.tableSchemaCache = make(map[string][]*dynamodb.KeySchemaElement)
	return &mg
}

// RunInTransaction runs ops in a transaction.
// The transaction is rolled back when ops returns an error.
// The transaction object, tx, passed into ops is safe for concurrent use by multiple goroutines.
// However, note that tx is only valid before ops returns, so it is important for ops to synchronize all goroutines that it spawns.
func (mg *TransactionManager) RunInTransaction(ops func(tx *Transaction) error) error {
	tx, err := mg.newTransaction()
	if err != nil {
		return errors.Wrap(err, "newTransaction")
	}

	err = ops(tx)
	if err != nil {
		rollbackErr := rollback(tx)
		if rollbackErr == nil {
			tx.txItem.delete(mg.ttlTimeout)
		}
		return err
	}

	commitErr := tx.commit()
	if commitErr != nil {
		rollbackErr := rollback(tx)
		if rollbackErr == nil {
			tx.txItem.delete(mg.ttlTimeout)
		}
		return errors.Wrap(commitErr, "commit")
	}

	tx.txItem.delete(mg.ttlTimeout)
	return nil
}

// Sweep cleans up failed transactions.
// Transactions whose last updated time plus rollbackDuration have passed are rolledback.
// It is important to choose a rollbackDuration that is long enough to guarantee that there are no coordinators working on the transaction when Sweep is called.
// Failing to do so might result in inconsistent item states.
// If shouldDel is true, the transaction is deleted after the transaction is successfully rolledback.
//
// A typical use case of this function is in the context of a background cleanup job.
// The job would scan the transactions table, and for each transaction item it encounters,
// pass it to Sweep with an appropriate rollbackDuration value,
// depending on the characteristics of the application.
func (mg *TransactionManager) Sweep(txAttrs map[string]*dynamodb.AttributeValue, rollbackDuration time.Duration, shouldDel bool) error {
	txItem, err := newTransactionItemByItem(txAttrs, mg)
	if err != nil {
		return errors.Wrap(err, "newTransactionItemByItem")
	}
	lut, err := lastUpdateTime(txItem.txItem)
	if err != nil {
		return errors.Wrap(err, "lastUpdateTime")
	}
	state, err := txItem.getState()
	if err != nil {
		return errors.Wrap(err, "getState")
	}

	if state == TransactionItemStatePending && time.Now().Before(lut.Add(rollbackDuration)) {
		return nil
	}

	tx := &Transaction{
		txManager: mg,
		txItem:    txItem,
		Retrier:   newDefaultJitterExpBackoff(),
	}
	if err := rollback(tx); err != nil {
		return errors.Wrap(err, "rollback")
	}

	if shouldDel {
		if err := txItem.delete(mg.ttlTimeout); err != nil {
			return errors.Wrap(err, "delete")
		}
	}
	return nil
}

type posItem struct {
	pos  int
	item map[string]*dynamodb.AttributeValue
}

// Query does a read operation at the read committed level
// https://en.wikipedia.org/wiki/Isolation_(database_systems) .
// For single item queries, this means only successfully committed changes are read.
// However, for range queries, phantom reads might occur.
func (mg *TransactionManager) Query(input *dynamodb.QueryInput) (*dynamodb.QueryOutput, error) {
	output, err := mg.client.Query(input)
	if err != nil {
		return nil, err
	}

	hchan := make(chan posItem)
	for i, item := range output.Items {
		go func(i int, item map[string]*dynamodb.AttributeValue) {
			handled, err := handleReadCommitted(item, *input.TableName, mg)
			if err != nil {
				hchan <- posItem{pos: i, item: nil}
				return
			}
			stripSpecialAttributes(handled)
			hchan <- posItem{pos: i, item: handled}
		}(i, item)
	}
	handledItems := make([]map[string]*dynamodb.AttributeValue, len(output.Items))
	for i := 0; i < len(output.Items); i++ {
		pi := <-hchan
		handledItems[pi.pos] = pi.item
	}

	nilFiltered := make([]map[string]*dynamodb.AttributeValue, 0, len(output.Items))
	for _, item := range handledItems {
		if item == nil {
			continue
		}
		nilFiltered = append(nilFiltered, item)
	}

	output.Items = nilFiltered
	return output, nil
}

// TransactionInfo returns information about a transaction given its ID.
// This information contains the state of the transaction as well as
// it's last updated time whose semantics are described below:
//
// If txState is TransactionItemStatePending, txTime is the transaction's
// creation time, or the time its last read or write request is issued.
// If txState is TransactionItemStateCommitted, txTime is the transaction's commit time.
// If txState is TransactionItemStateRolledBack, txTime is the transaction's rollback time.
func (mg *TransactionManager) TransactionInfo(id string) (txState string, txTime time.Time, err error) {
	txItem, err := newTransactionItem(id, manager, false)
	if err != nil {
		return "", time.Unix(0, 0), errors.Wrap(err, "newTransactionItem")
	}
	txState, err = txItem.getState()
	if err != nil {
		return "", time.Unix(0, 0), errors.Wrap(err, "getState")
	}
	txTime, err = lastUpdateTime(txItem.txItem)
	if err != nil {
		return "", time.Unix(0, 0), errors.Wrap(err, "lastUpdateTime")
	}
	return txState, txTime, nil
}

func (mg *TransactionManager) newTransaction() (*Transaction, error) {
	item, err := newTransactionItem(randString(16), mg, true)
	if err != nil {
		return nil, errors.Wrap(err, "newTransactionItem")
	}
	tx := &Transaction{
		txManager: mg,
		txItem:    item,
		Retrier:   newDefaultJitterExpBackoff(),
	}
	return tx, nil
}

// VerifyOrCreateTransactionTable ensures that the table containing transactions exists.
// However, note that it creates the table with a provision throughput of 1, which you might want to adjust.
func (mg *TransactionManager) VerifyOrCreateTransactionTable() error {
	return verifyOrCreateTable(mg.client, mg.transactionTableName, transactionsTableAttributes, transactionsTableKeySchema)
}

// VerifyOrCreateTransactionImagesTable ensures that the table containing transaction item images exists.
// However, note that it creates the table with a provision throughput of 1, which you might want to adjust.
func (mg *TransactionManager) VerifyOrCreateTransactionImagesTable() error {
	return verifyOrCreateTable(mg.client, mg.itemImageTableName, transactionsImagesTableAttributes, transactionsImagesTableKeySchema)
}

func (mg *TransactionManager) getTableSchema(tableName string) ([]*dynamodb.KeySchemaElement, error) {
	mg.mutex.RLock()
	schema, ok := mg.tableSchemaCache[tableName]
	mg.mutex.RUnlock()
	if ok {
		return schema, nil
	}

	mg.mutex.Lock()
	defer mg.mutex.Unlock()

	req := &dynamodb.DescribeTableInput{
		TableName: &tableName,
	}
	resp, err := mg.client.DescribeTable(req)
	if err != nil {
		return nil, errors.Wrap(err, "DescribeTable")
	}
	schema = resp.Table.KeySchema
	mg.tableSchemaCache[tableName] = schema
	return schema, nil
}

func (mg *TransactionManager) createKeyMap(tableName string, item map[string]*dynamodb.AttributeValue) (map[string]*dynamodb.AttributeValue, error) {
	schema, err := mg.getTableSchema(tableName)
	if err != nil {
		return nil, errors.Wrap(err, "getTableSchema")
	}

	key := make(map[string]*dynamodb.AttributeValue)
	for _, kse := range schema {
		name := *kse.AttributeName
		val, ok := item[name]
		if !ok {
			return nil, fmt.Errorf("item has no attribute: %s", name)
		}
		key[name] = val
	}
	return key, nil
}

func (mg *TransactionManager) getCurrentTimeAttribute() *dynamodb.AttributeValue {
	n := timeToStr(time.Now())
	val := dynamodb.AttributeValue{N: &n}
	return &val
}

func verifyOrCreateTable(client dynamodbiface.DynamoDBAPI, tableName string, attrDefs []*dynamodb.AttributeDefinition, keySchema []*dynamodb.KeySchemaElement) error {
	var tableDesc *dynamodb.TableDescription
	resp, err := client.DescribeTable(&dynamodb.DescribeTableInput{TableName: &tableName})
	if err != nil {
		awsErr, ok := err.(awserr.Error)
		if !ok {
			return errors.Wrap(err, "DescribeTable")
		}
		if awsErr.Code() != "ResourceNotFoundException" {
			return errors.Wrap(awsErr, "DescribeTable")
		}
	}
	if err != nil {
		provTP := &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(1),
			WriteCapacityUnits: aws.Int64(1),
		}
		ctInput := &dynamodb.CreateTableInput{
			TableName:             &tableName,
			KeySchema:             keySchema,
			AttributeDefinitions:  attrDefs,
			ProvisionedThroughput: provTP,
		}
		ctResp, err := client.CreateTable(ctInput)
		if err != nil {
			return errors.Wrap(err, "CreateTable")
		}
		tableDesc = ctResp.TableDescription
	} else {
		tableDesc = resp.Table
	}

	if err := verifyTableDescription(tableDesc, attrDefs, keySchema); err != nil {
		return errors.Wrap(err, "verifyTableDescription")
	}

	return nil
}

func verifyTableDescription(table *dynamodb.TableDescription, attrDefs []*dynamodb.AttributeDefinition, keySchema []*dynamodb.KeySchemaElement) error {
	toADMap := func(ads []*dynamodb.AttributeDefinition) map[string]struct{} {
		adMap := make(map[string]struct{})
		for _, def := range ads {
			k := fmt.Sprintf("%s%s", *def.AttributeName, *def.AttributeType)
			adMap[k] = struct{}{}
		}
		return adMap
	}
	attrDefsMap := toADMap(attrDefs)
	tbAttrDefsMap := toADMap(table.AttributeDefinitions)
	if !strMapsEqual(attrDefsMap, tbAttrDefsMap) {
		return fmt.Errorf("different attribute definitions")
	}

	toKSMap := func(ksma []*dynamodb.KeySchemaElement) map[string]struct{} {
		ksMap := make(map[string]struct{})
		for _, ks := range ksma {
			k := fmt.Sprintf("%s%s", *ks.AttributeName, *ks.KeyType)
			ksMap[k] = struct{}{}
		}
		return ksMap
	}
	ksMap := toKSMap(keySchema)
	tbKSMap := toKSMap(table.KeySchema)
	if !strMapsEqual(ksMap, tbKSMap) {
		return fmt.Errorf("different key schemas %+v %+v", ksMap, tbKSMap)
	}

	return nil
}

func strMapsEqual(a, b map[string]struct{}) bool {
	if len(a) != len(b) {
		return false
	}
	for k, _ := range a {
		if _, ok := b[k]; !ok {
			return false
		}
	}
	return true
}
