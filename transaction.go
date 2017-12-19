package dtx

import (
	"fmt"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/golang/glog"
	"github.com/pkg/errors"

	ddb "github.com/fumin/dtx/dynamodb"
)

const (
	// The transaction algorithm this package uses adds several extra fields to items it manages.
	// These fields all start with the prefix TxAttrPrefix.
	// Note that the correctness of the algorithm depends heavily on these fields,
	// and any direct updates to them might result in inconsistencies that can only be fixed via manual intervention.
	TxAttrPrefix = "Tx_"

	// AttributeNameTxID is the unique identifier of a transaction.
	// It is also the primary HASH key of the transaction table.
	AttributeNameTxID = TxAttrPrefix + "Id" // follow the Java version

	// AttributeNameTransient indicates whether an item is transient.
	// Transient items do not appear in read consistent queries,
	// since they might be deleted should a transaction rolls back.
	AttributeNameTransient = TxAttrPrefix + "T"

	// AttributeNameDate is a field in a transaction item,
	// indicating the last time it was updated.
	AttributeNameDate = TxAttrPrefix + "D"

	// AttributeNameApplied indicates whether an item has been applied.
	// This field is used internally when taking item snapshots for rollbacks.
	AttributeNameApplied = TxAttrPrefix + "A"

	// AttributeNameRequests is a field in a transaction item.
	// It consists of the requests that have participating in the transaction.
	AttributeNameRequests = TxAttrPrefix + "R"

	// AttributeNameState is the state of a transaction.
	// Possible values are listed in the constants TransactionItemState*.
	AttributeNameState = TxAttrPrefix + "S"

	// AttributeNameVersion is the version of a transaction.
	// Every modification to a transaction increments its version.
	AttributeNameVersion = TxAttrPrefix + "V"

	// AttributeNameImageID is the unique identifier of an item image.
	// An item image is a snapshot of an item when a transaction begins.
	// It is also the state in which an item is revert to should a transaction rolls back.
	AttributeNameImageID = TxAttrPrefix + "I"

	// AttributeNameTTL is the Time To Live of a transaction.
	// Instead of an immediate removal, the deletion of a transaction
	// is done by setting its TTL.
	AttributeNameTTL = TxAttrPrefix + "L"

	booleanTrueAttrVal = "1"
)

var (
	// SpecialAttrNames are the attribute names managed by this package.
	// Any direct updates to them might result in inconsistencies that can only be fixed via manual intervention.
	SpecialAttrNames = []string{
		AttributeNameTxID,
		AttributeNameTransient,
		AttributeNameDate,
		AttributeNameApplied,
		AttributeNameRequests,
		AttributeNameState,
		AttributeNameVersion,
		AttributeNameImageID,
	}

	// Reserved ExpressionAttributeValue names
	exprNameTxID      = ":exprNameTxID"
	exprNameDate      = ":exprNameDate"
	exprNameApplied   = ":exprNameApplied"
	exprNameTransient = ":exprNameTransient"
	exprNameImageID   = ":exprNameImageID"
)

type notFoundError struct {
	tableName string
	key       map[string]*dynamodb.AttributeValue
}

func (e *notFoundError) Error() string {
	return fmt.Sprintf("item not found in table %s key %+v", e.tableName, e.key)
}

// A Transaction groups multiple operations so that either all of them fail or all of them succeed together.
// In addition, reads in a transaction are isolated from other transactions,
// in that it is guaranteed that items are not updated by other transactions
// between the time it is read and when the transaction successfully commits.
//
// Note that when using transactions on items in a table, you should always perform write operations using this transaction library.
// Failing to do so might cause data inconsistencies that can only be resolved through manual intervention.
type Transaction struct {
	mutex sync.RWMutex

	txManager *TransactionManager
	txItem    *transactionItem
	txErr     error

	// During contention with other transactions,
	// a transaction waits for a while, rolls back the competing transaction, and retries an operation.
	// The Retrier of a transaction controls this behaviour.
	Retrier Retrier
}

// ID returns the unique identifier of a transaction.
func (tx *Transaction) ID() string {
	return tx.txItem.id
}

// PutItem adds a PutItem operation to a transaction.
func (tx *Transaction) PutItem(input *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
	tx.mutex.Lock()
	defer tx.mutex.Unlock()
	if tx.txErr != nil {
		return nil, tx.txErr
	}

	res, err := putItem(tx, input)
	if err != nil {
		tx.txErr = errors.Wrap(err, "putItem")
		return nil, err
	}
	return res, nil
}

// UpdateItem adds an UpdateItem operation to a transaction.
func (tx *Transaction) UpdateItem(input *dynamodb.UpdateItemInput) (*dynamodb.UpdateItemOutput, error) {
	tx.mutex.Lock()
	defer tx.mutex.Unlock()
	if tx.txErr != nil {
		return nil, tx.txErr
	}

	res, err := updateItem(tx, input)
	if err != nil {
		tx.txErr = errors.Wrap(err, "updateItem")
		return nil, err
	}
	return res, nil
}

// DeleteItem adds a DeleteItem operation to a transaction.
func (tx *Transaction) DeleteItem(input *dynamodb.DeleteItemInput) (*dynamodb.DeleteItemOutput, error) {
	tx.mutex.Lock()
	defer tx.mutex.Unlock()
	if tx.txErr != nil {
		return nil, tx.txErr
	}

	res, err := deleteItem(tx, input)
	if err != nil {
		tx.txErr = errors.Wrap(err, "deleteItem")
		return nil, err
	}
	return res, nil
}

// GetItem performs a GetItem operation in a transaction.
// The item involved in the operation is guratanteed to be not modified by
// other transactions during the lifetime of the current transaction.
func (tx *Transaction) GetItem(input *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
	tx.mutex.Lock()
	defer tx.mutex.Unlock()
	if tx.txErr != nil {
		return nil, tx.txErr
	}

	res, err := getItem(tx, input)
	if err != nil {
		tx.txErr = errors.Wrap(err, "getItem")
		return nil, err
	}
	return res, nil
}

func (tx *Transaction) commit() error {
	tx.mutex.Lock()
	defer tx.mutex.Unlock()
	if tx.txErr != nil {
		return tx.txErr
	}

	finishErr := tx.txItem.finish(TransactionItemStateCommitted)
	if finishErr != nil {
		// The most likely reason why we failed is because we are rolled back.
		// Ascertain whether this is the case to provide more informative errors.
		txItem, err := newTransactionItem(tx.txItem.id, tx.txManager, false)
		if err != nil {
			return errors.Wrap(finishErr, "finish")
		}
		tx.txItem = txItem
		state, err := tx.txItem.getState()
		if err != nil {
			return errors.Wrap(err, "getState")
		}
		if state == TransactionItemStateRolledBack {
			return &TransactionRolledBackError{txItem: tx.txItem}
		}
		return errors.Wrap(finishErr, "finish")
	}

	doCommit(tx)
	return nil
}

func rollback(tx *Transaction) error {
	tx.mutex.Lock()
	defer tx.mutex.Unlock()

	fn, fnName, err := func() (func(*Transaction) error, string, error) {
		err := tx.txItem.finish(TransactionItemStateRolledBack)
		if err == nil {
			return doRollback, "doRollback", nil
		}

		txItem, err := newTransactionItem(tx.txItem.id, tx.txManager, false)
		if err != nil {
			return nil, "", errors.Wrap(err, "newTransactionItem")
		}
		tx.txItem = txItem
		state, err := tx.txItem.getState()
		if err != nil {
			return nil, "", errors.Wrap(err, "getState")
		}
		switch state {
		case TransactionItemStateRolledBack:
			return doRollback, "doRollback", nil
		case TransactionItemStateCommitted:
			return doCommit, "doCommit", nil
		default:
			return nil, "", fmt.Errorf("state not able to rollback %s", state)
		}
	}()
	if err != nil {
		return err
	}

	if err := fn(tx); err != nil {
		return errors.Wrap(err, fnName)
	}
	return nil
}

func putItem(tx *Transaction, input *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
	wrappedRequest, err := newPutItemRequest(input, tx.txManager)
	if err != nil {
		return nil, errors.Wrap(err, "newPutItemRequest")
	}
	item, err := driveRequest(tx, wrappedRequest)
	if err != nil {
		return nil, errors.Wrap(err, "driveRequest")
	}
	stripSpecialAttributes(item)
	output := &dynamodb.PutItemOutput{
		Attributes: item,
	}
	return output, nil
}

func updateItem(tx *Transaction, input *dynamodb.UpdateItemInput) (*dynamodb.UpdateItemOutput, error) {
	wrappedRequest, err := newUpdateItemRequest(input)
	if err != nil {
		return nil, errors.Wrap(err, "newUpdateItemRequest")
	}
	item, err := driveRequest(tx, wrappedRequest)
	if err != nil {
		return nil, errors.Wrap(err, "driveRequest")
	}
	stripSpecialAttributes(item)
	output := &dynamodb.UpdateItemOutput{
		Attributes: item,
	}
	return output, nil
}

func deleteItem(tx *Transaction, input *dynamodb.DeleteItemInput) (*dynamodb.DeleteItemOutput, error) {
	wrappedRequest, err := newDeleteItemRequest(input)
	if err != nil {
		return nil, errors.Wrap(err, "newDeleteItemRequest")
	}
	item, err := driveRequest(tx, wrappedRequest)
	if err != nil {
		return nil, errors.Wrap(err, "driveRequest")
	}
	stripSpecialAttributes(item)
	output := &dynamodb.DeleteItemOutput{
		Attributes: item,
	}
	return output, nil
}

func getItem(tx *Transaction, input *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
	wrappedRequest, err := newGetItemRequest(input)
	if err != nil {
		return nil, errors.Wrap(err, "newGetItemRequest")
	}
	item, err := driveRequest(tx, wrappedRequest)
	if err != nil {
		return nil, errors.Wrap(err, "driveRequest")
	}
	stripSpecialAttributes(item)
	output := &dynamodb.GetItemOutput{
		Item: item,
	}
	return output, nil
}

func driveRequest(tx *Transaction, clientRequest txRequest) (map[string]*dynamodb.AttributeValue, error) {
	if err := validateRequest(clientRequest, tx.txItem.id, tx.txManager); err != nil {
		return nil, errors.Wrap(err, "validateRequest")
	}

	if err := addRequest(tx, clientRequest); err != nil {
		return nil, errors.Wrap(err, "addRequest")
	}

	item, err := lockAndApply(tx, clientRequest)
	if err != nil {
		return nil, errors.Wrap(err, "lockAndApply")
	}
	return item, nil
}

func probeToBeLockedItem(tx *Transaction, req txRequest) (bool, *Transaction, error) {
	expectExists := false
	item, owner, err := getItemTx(tx.txManager, req)
	if err != nil {
		return expectExists, nil, nil
	}
	if item == nil {
		return expectExists, nil, nil
	}
	expectExists = true

	if owner == nil {
		return expectExists, nil, nil
	}
	if owner.txItem.id == tx.txItem.id {
		tx.txItem = owner.txItem
		state, err := tx.txItem.getState()
		if err != nil {
			return expectExists, nil, errors.Wrap(err, "getState")
		}
		switch state {
		case TransactionItemStatePending:
			return expectExists, nil, nil
		case TransactionItemStateRolledBack:
			return expectExists, nil, &TransactionRolledBackError{txItem: tx.txItem}
		case TransactionItemStateCommitted:
			return expectExists, nil, &TransactionCommittedError{txItem: tx.txItem}
		default:
			return expectExists, nil, fmt.Errorf("unknown state %s", state)
		}
	}

	return expectExists, owner, nil
}

func lockAndApply(tx *Transaction, clientRequest txRequest) (map[string]*dynamodb.AttributeValue, error) {
	var item map[string]*dynamodb.AttributeValue
	var lockErr error
	expectExists := true
	tx.Retrier.Reset()
	for {
		item, lockErr = lockAndApplyLogic(tx, clientRequest, expectExists)
		if lockErr == nil {
			break
		}

		// Do not retry if error is not a transaction race condition.
		awsErr, ok := errors.Cause(lockErr).(awserr.Error)
		if !ok {
			break
		}
		if awsErr.Code() != "ConditionalCheckFailedException" {
			break
		}

		var otherTransaction *Transaction
		var err error
		expectExists, otherTransaction, err = probeToBeLockedItem(tx, clientRequest)
		if err != nil {
			return nil, errors.Wrap(err, "probeToBeLockedItem")
		}

		if !tx.Retrier.Wait() {
			break
		}
		if otherTransaction != nil {
			rollback(otherTransaction)
		}
	}
	if lockErr != nil {
		return nil, lockErr
	}
	return item, nil
}

func lockAndApplyLogic(tx *Transaction, callerRequest txRequest, expectExists bool) (map[string]*dynamodb.AttributeValue, error) {
	lockedItem, err := lockItem(tx, callerRequest, expectExists)
	if err != nil {
		return nil, errors.Wrap(err, "lockItem")
	}

	// Take a snapshot of the item, if it has not been applied.
	_, isApplied := lockedItem[AttributeNameApplied]
	_, isTransient := lockedItem[AttributeNameTransient]
	_, isGet := callerRequest.(*getItemRequest)
	if !isApplied && !isTransient && !isGet {
		if err := tx.txItem.saveItemImage(lockedItem, callerRequest.getRid()); err != nil {
			return nil, errors.Wrap(err, "saveItemImage")
		}
	}

	returnItem, err := applyAndKeepLock(tx, callerRequest, lockedItem)
	if err != nil {
		return nil, errors.Wrap(err, "applyAndKeepLock")
	}

	return returnItem, nil
}

func addRequest(tx *Transaction, callerRequest txRequest) error {
	arErr := tx.txItem.addRequest(callerRequest)
	if arErr == nil {
		return nil
	}

	// The most likely reason why addRequest would fail is our transaction being rolled back.
	// Ascertain whether this is the case in order to provide more informative errors.
	txItem, err := newTransactionItem(tx.txItem.id, tx.txManager, false)
	if err != nil {
		return arErr
	}
	tx.txItem = txItem
	state, err := tx.txItem.getState()
	if err != nil {
		return errors.Wrap(err, "getState")
	}
	if state == TransactionItemStateCommitted {
		return &TransactionCommittedError{txItem: tx.txItem}
	}
	if state == TransactionItemStateRolledBack {
		return &TransactionRolledBackError{txItem: tx.txItem}
	}
	return arErr
}

func applyAndKeepLock(tx *Transaction, callerRequest txRequest, lockedItem map[string]*dynamodb.AttributeValue) (map[string]*dynamodb.AttributeValue, error) {
	if _, ok := callerRequest.(*getItemRequest); ok {
		lockingReq := getReqInMap(callerRequest.getTableName(), callerRequest.getImmutableKey(), tx.txItem.requestsMap)
		if lockingReq == nil {
			return lockedItem, nil
		}
		if _, ok := lockingReq.(*deleteItemRequest); ok {
			return nil, nil
		}
		_, isGet := lockingReq.(*getItemRequest)
		_, isTransient := lockedItem[AttributeNameTransient]
		if isGet && isTransient {
			return nil, nil
		}
		return lockedItem, nil
	}

	if _, ok := callerRequest.(*deleteItemRequest); ok {
		return nil, nil
	}

	_, isApplied := lockedItem[AttributeNameApplied]
	if isApplied {
		return lockedItem, nil
	}

	// Don't mutate the caller's request.
	copiedReq, err := copyRequest(callerRequest)
	if err != nil {
		return nil, errors.Wrap(err, "copyRequest")
	}

	condExprAnds := make([]string, 0, 2)
	eav := make(map[string]*dynamodb.AttributeValue)
	eav[exprNameTxID] = &dynamodb.AttributeValue{S: &tx.txItem.id}
	condExprAnds = append(condExprAnds, fmt.Sprintf("%s = %s", AttributeNameTxID, exprNameTxID))
	condExprAnds = append(condExprAnds, fmt.Sprintf("attribute_not_exists(%s)", AttributeNameApplied))
	if putReq, ok := copiedReq.(*putItemRequest); ok {
		input := putReq.Input
		input.Item[AttributeNameTxID] = &dynamodb.AttributeValue{S: &tx.txItem.id}
		input.Item[AttributeNameApplied] = &dynamodb.AttributeValue{S: aws.String(booleanTrueAttrVal)}
		if tav, isTransient := lockedItem[AttributeNameTransient]; isTransient {
			input.Item[AttributeNameTransient] = tav
		}
		input.Item[AttributeNameDate] = lockedItem[AttributeNameDate]
		if input.ExpressionAttributeValues == nil {
			input.ExpressionAttributeValues = make(map[string]*dynamodb.AttributeValue)
		}
		for k, v := range eav {
			input.ExpressionAttributeValues[k] = v
		}
		condExpr := strings.Join(condExprAnds, " AND ")
		if input.ConditionExpression == nil {
			input.ConditionExpression = aws.String(condExpr)
		} else {
			input.ConditionExpression = aws.String(fmt.Sprintf("(%s) AND %s", *input.ConditionExpression, condExpr))
		}
		resp, err := tx.txManager.client.PutItem(input)
		if err != nil {
			return nil, errors.Wrap(err, "PutItem")
		}
		return resp.Attributes, nil
	}

	if updateReq, ok := copiedReq.(*updateItemRequest); ok {
		eav[exprNameApplied] = &dynamodb.AttributeValue{S: aws.String(booleanTrueAttrVal)}
		sets := []string{fmt.Sprintf("%s = %s", AttributeNameApplied, exprNameApplied)}

		input := updateReq.Input
		if input.ExpressionAttributeValues == nil {
			input.ExpressionAttributeValues = make(map[string]*dynamodb.AttributeValue)
		}
		for k, v := range eav {
			input.ExpressionAttributeValues[k] = v
		}
		condExpr := strings.Join(condExprAnds, " AND ")
		if input.ConditionExpression == nil {
			input.ConditionExpression = aws.String(condExpr)
		} else {
			input.ConditionExpression = aws.String(fmt.Sprintf("(%s) AND %s", *input.ConditionExpression, condExpr))
		}

		updateExpr := ddb.ParseUpdateExpr(input.UpdateExpression)
		updateExpr.Set = append(updateExpr.Set, sets...)
		input.UpdateExpression = aws.String(updateExpr.String())

		resp, err := tx.txManager.client.UpdateItem(input)
		if err != nil {
			return nil, errors.Wrap(err, "UpdateItem")
		}
		return resp.Attributes, nil
	}

	return nil, fmt.Errorf("should not reach here")
}

func lockItem(tx *Transaction, callerRequest txRequest, expectExists bool) (map[string]*dynamodb.AttributeValue, error) {
	eav := make(map[string]*dynamodb.AttributeValue)
	sets := make([]string, 0)
	eav[exprNameTxID] = &dynamodb.AttributeValue{S: &tx.txItem.id}
	sets = append(sets, fmt.Sprintf("%s = %s", AttributeNameTxID, exprNameTxID))
	eav[exprNameDate] = tx.txManager.getCurrentTimeAttribute()
	sets = append(sets, fmt.Sprintf("%s = %s", AttributeNameDate, exprNameDate))

	condExprAnds := make([]string, 0, 1)
	if expectExists {
		condExprAnds = append(condExprAnds, fmt.Sprintf("(attribute_not_exists(%s) OR %s = %s)", AttributeNameTxID, AttributeNameTxID, exprNameTxID))
		for k, v := range callerRequest.getKey() {
			exprName := fmt.Sprintf(":%s%s", k, randString(4))
			eav[exprName] = v
			condExprAnds = append(condExprAnds, fmt.Sprintf("%s = %s", k, exprName))
		}
	} else {
		condExprAnds = append(condExprAnds, fmt.Sprintf("attribute_not_exists(%s)", AttributeNameTxID))
		for k, _ := range callerRequest.getKey() {
			condExprAnds = append(condExprAnds, fmt.Sprintf("attribute_not_exists(%s)", k))
		}
		eav[exprNameTransient] = &dynamodb.AttributeValue{S: aws.String(booleanTrueAttrVal)}
		sets = append(sets, fmt.Sprintf("%s = %s", AttributeNameTransient, exprNameTransient))
	}

	updateExpr := "SET " + strings.Join(sets, ", ")
	conditionExpr := strings.Join(condExprAnds, " AND ")
	input := &dynamodb.UpdateItemInput{
		TableName: aws.String(callerRequest.getTableName()),
		Key:       callerRequest.getKey(),
		ExpressionAttributeValues: eav,
		ConditionExpression:       &conditionExpr,
		UpdateExpression:          &updateExpr,
		ReturnValues:              aws.String("ALL_NEW"),
	}
	resp, err := tx.txManager.client.UpdateItem(input)
	if err != nil {
		return nil, errors.Wrap(err, "UpdateItem")
	}
	return resp.Attributes, nil
}

func doCommit(tx *Transaction) error {
	state, err := tx.txItem.getState()
	if err != nil {
		return errors.Wrap(err, "getState")
	}
	if state != TransactionItemStateCommitted {
		return fmt.Errorf("wrong state %s", state)
	}

	for _, req := range tx.txItem.getRequestsSorted() {
		if err := unlockItemAfterCommit(tx, req); err != nil {
			return errors.Wrap(err, "unlockItemAfterCommit")
		}
	}

	for _, req := range tx.txItem.getRequestsSorted() {
		if err := tx.txItem.deleteItemImage(req.getRid()); err != nil {
			return errors.Wrap(err, "deleteItemImage")
		}
	}
	return nil
}

func doRollback(tx *Transaction) error {
	state, err := tx.txItem.getState()
	if err != nil {
		return errors.Wrap(err, "getState")
	}
	if state != TransactionItemStateRolledBack {
		return fmt.Errorf("wrong state %s", state)
	}

	for _, req := range tx.txItem.getRequestsSorted() {
		if err := rollbackItemAndReleaseLock(tx, req); err != nil {
			return errors.Wrap(err, "rollbackItemAndReleaseLock")
		}

		if err := tx.txItem.deleteItemImage(req.getRid()); err != nil {
			return errors.Wrap(err, "deleteItemImage")
		}
	}
	return nil
}

func rollbackItemAndReleaseLock(tx *Transaction, request txRequest) error {
	_, isGet := request.(*getItemRequest)
	rid := request.getRid()

	// Check item needs to be handled.
	item, err := getItemByKey(tx.txManager, request.getTableName(), request.getKey())
	if err != nil {
		if _, ok := err.(*notFoundError); ok {
			return nil
		}
		return errors.Wrap(err, "getItemByKey")
	}
	owner := txGetOwner(item)
	if owner != tx.txItem.id {
		return nil
	}

	if isGet {
		if err := releaseReadLock(tx, request, item); err != nil {
			return errors.Wrap(err, "releaseReadLock")
		}
		return nil
	}

	itemImage, err := tx.txItem.loadItemImage(rid)
	if err != nil {
		return errors.Wrap(err, "loadItemImage")
	}
	// We have a backup, restore it.
	if itemImage != nil {
		if _, ok := itemImage[AttributeNameTransient]; ok {
			return fmt.Errorf("image should not be transient")
		}
		if _, ok := itemImage[AttributeNameApplied]; ok {
			return fmt.Errorf("image should not be applied")
		}
		delete(itemImage, AttributeNameTxID)
		delete(itemImage, AttributeNameDate)
		eav := make(map[string]*dynamodb.AttributeValue)
		eav[exprNameTxID] = &dynamodb.AttributeValue{S: &tx.txItem.id}
		condExpr := fmt.Sprintf("%s = %s", AttributeNameTxID, exprNameTxID)
		input := &dynamodb.PutItemInput{
			TableName: aws.String(request.getTableName()),
			Item:      itemImage,
			ExpressionAttributeValues: eav,
			ConditionExpression:       &condExpr,
		}
		_, err := tx.txManager.client.PutItem(input)
		if err != nil {
			return errors.Wrap(err, "PutItem")
		}
		return nil
	}

	// We do not have a backup, but we know we are transient.
	if _, ok := item[AttributeNameTransient]; ok {
		eav := make(map[string]*dynamodb.AttributeValue)
		condExprAnds := make([]string, 0, 2)
		eav[exprNameTxID] = &dynamodb.AttributeValue{S: &tx.txItem.id}
		condExprAnds = append(condExprAnds, fmt.Sprintf("%s = %s", AttributeNameTxID, exprNameTxID))
		eav[exprNameTransient] = &dynamodb.AttributeValue{S: aws.String(booleanTrueAttrVal)}
		condExprAnds = append(condExprAnds, fmt.Sprintf("%s = %s", AttributeNameTransient, exprNameTransient))
		condExpr := strings.Join(condExprAnds, " AND ")
		input := &dynamodb.DeleteItemInput{
			TableName: aws.String(request.getTableName()),
			Key:       request.getKey(),
			ExpressionAttributeValues: eav,
			ConditionExpression:       &condExpr,
		}
		_, err := tx.txManager.client.DeleteItem(input)
		if err != nil {
			return errors.Wrap(err, "DeleteItem")
		}
		return nil
	}

	// We do not have a backup, nor are we transient.
	// This can only happen when the item has been locked,
	// but the transaction has yet to apply a Put or a Update request.
	if _, ok := item[AttributeNameApplied]; ok {
		glog.Warningf("item applied but does not have a backup %+v", item)
		return fmt.Errorf("item has been applied %+v", item)
	}
	if err := releaseReadLock(tx, request, item); err != nil {
		return errors.Wrap(err, "releaseReadLock")
	}
	return nil
}

func unlockItemAfterCommit(tx *Transaction, request txRequest) error {
	// Check if item needs to be handled.
	item, err := getItemByKey(tx.txManager, request.getTableName(), request.getKey())
	if err != nil {
		if _, ok := err.(*notFoundError); ok {
			return nil
		}
		return errors.Wrap(err, "getItemByKey")
	}
	owner := txGetOwner(item)
	if owner != tx.txItem.id {
		return nil
	}

	eav := make(map[string]*dynamodb.AttributeValue)
	eav[exprNameTxID] = &dynamodb.AttributeValue{S: &tx.txItem.id}
	condExprAnds := make([]string, 0, 1)
	condExprAnds = append(condExprAnds, fmt.Sprintf("%s = %s", AttributeNameTxID, exprNameTxID))

	_, isPut := request.(*putItemRequest)
	_, isUpdate := request.(*updateItemRequest)
	_, isDelete := request.(*deleteItemRequest)
	_, isGet := request.(*getItemRequest)
	if isPut || isUpdate {
		eav[exprNameApplied] = &dynamodb.AttributeValue{S: aws.String(booleanTrueAttrVal)}
		condExprAnds = append(condExprAnds, fmt.Sprintf("%s = %s", AttributeNameApplied, exprNameApplied))
		rems := make([]string, 0, 4)
		rems = append(rems, AttributeNameTxID)
		rems = append(rems, AttributeNameTransient)
		rems = append(rems, AttributeNameApplied)
		rems = append(rems, AttributeNameDate)
		condExpr := strings.Join(condExprAnds, " AND ")
		updateExpr := fmt.Sprintf("REMOVE %s", strings.Join(rems, ", "))
		input := &dynamodb.UpdateItemInput{
			TableName: aws.String(request.getTableName()),
			Key:       request.getKey(),
			ExpressionAttributeValues: eav,
			ConditionExpression:       &condExpr,
			UpdateExpression:          &updateExpr,
		}
		_, err := tx.txManager.client.UpdateItem(input)
		if err != nil {
			return errors.Wrap(err, "UpdateItem")
		}
	} else if isDelete {
		condExpr := strings.Join(condExprAnds, " AND ")
		input := &dynamodb.DeleteItemInput{
			TableName: aws.String(request.getTableName()),
			Key:       request.getKey(),
			ExpressionAttributeValues: eav,
			ConditionExpression:       &condExpr,
		}
		_, err := tx.txManager.client.DeleteItem(input)
		if err != nil {
			return errors.Wrap(err, "DeleteItem")
		}
	} else if isGet {
		err := releaseReadLock(tx, request, item)
		if err != nil {
			return errors.Wrap(err, "releaseReadLock")
		}
	} else {
		return fmt.Errorf("unknown request type %+v", request)
	}
	return nil
}

func releaseReadLock(tx *Transaction, req txRequest, item map[string]*dynamodb.AttributeValue) error {
	_, isTransient := item[AttributeNameTransient]
	if isTransient {
		if err := releaseReadLockTransient(tx, req); err != nil {
			return errors.Wrap(err, "releaseReadLockTransient")
		}
		return nil
	}

	if err := releaseReadLockNonTransient(tx, req); err != nil {
		return errors.Wrap(err, "releaseReadLockNonTransient")
	}
	return nil
}

func releaseReadLockNonTransient(tx *Transaction, req txRequest) error {
	eav := make(map[string]*dynamodb.AttributeValue)
	condExprAnds := make([]string, 0, 3)
	eav[exprNameTxID] = &dynamodb.AttributeValue{S: &tx.txItem.id}
	condExprAnds = append(condExprAnds, fmt.Sprintf("%s = %s", AttributeNameTxID, exprNameTxID))
	condExprAnds = append(condExprAnds, fmt.Sprintf("attribute_not_exists(%s)", AttributeNameTransient))
	condExprAnds = append(condExprAnds, fmt.Sprintf("attribute_not_exists(%s)", AttributeNameApplied))
	condExpr := strings.Join(condExprAnds, " AND ")
	rems := make([]string, 0, 2)
	rems = append(rems, AttributeNameTxID)
	rems = append(rems, AttributeNameDate)
	updateExpr := fmt.Sprintf("REMOVE %s", strings.Join(rems, ", "))
	input := &dynamodb.UpdateItemInput{
		TableName: aws.String(req.getTableName()),
		Key:       req.getKey(),
		ExpressionAttributeValues: eav,
		ConditionExpression:       &condExpr,
		UpdateExpression:          &updateExpr,
	}
	_, err := tx.txManager.client.UpdateItem(input)
	if err != nil {
		return errors.Wrap(err, "UpdateItem")
	}
	return nil
}

func releaseReadLockTransient(tx *Transaction, req txRequest) error {
	eav := make(map[string]*dynamodb.AttributeValue)
	condExprAnds := make([]string, 0, 3)
	eav[exprNameTxID] = &dynamodb.AttributeValue{S: &tx.txItem.id}
	condExprAnds = append(condExprAnds, fmt.Sprintf("%s = %s", AttributeNameTxID, exprNameTxID))
	eav[exprNameTransient] = &dynamodb.AttributeValue{S: aws.String(booleanTrueAttrVal)}
	condExprAnds = append(condExprAnds, fmt.Sprintf("%s = %s", AttributeNameTransient, exprNameTransient))
	condExprAnds = append(condExprAnds, fmt.Sprintf("attribute_not_exists(%s)", AttributeNameApplied))
	condExpr := strings.Join(condExprAnds, " AND ")
	input := &dynamodb.DeleteItemInput{
		TableName: aws.String(req.getTableName()),
		Key:       req.getKey(),
		ExpressionAttributeValues: eav,
		ConditionExpression:       &condExpr,
	}
	_, err := tx.txManager.client.DeleteItem(input)
	if err != nil {
		return errors.Wrap(err, "DeleteItem")
	}
	return nil
}

func stripSpecialAttributes(item map[string]*dynamodb.AttributeValue) {
	if item == nil {
		return
	}
	for _, attr := range SpecialAttrNames {
		delete(item, attr)
	}
}

func getItemTx(txManager *TransactionManager, req txRequest) (map[string]*dynamodb.AttributeValue, *Transaction, error) {
	item, err := getItemByKey(txManager, req.getTableName(), req.getKey())
	if err != nil {
		if _, ok := err.(*notFoundError); ok {
			return nil, nil, nil
		}
		return nil, nil, errors.Wrap(err, "getItemByKey")
	}

	owner := txGetOwner(item)
	ownerTxItem, err := newTransactionItem(owner, txManager, false)
	if err != nil {
		return nil, nil, errors.Wrap(err, "newTransactionItem")
	}
	tx := &Transaction{
		txManager: txManager,
		txItem:    ownerTxItem,
		Retrier:   newDefaultJitterExpBackoff(),
	}
	return item, tx, nil
}

func getItemByKey(txManager *TransactionManager, tableName string, key map[string]*dynamodb.AttributeValue) (map[string]*dynamodb.AttributeValue, error) {
	input := &dynamodb.GetItemInput{
		TableName:      &tableName,
		Key:            key,
		ConsistentRead: aws.Bool(true),
	}
	resp, err := txManager.client.GetItem(input)
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			if awsErr.Code() == "ValidationException" {
				return nil, &notFoundError{tableName: tableName, key: key}
			}
		}
		return nil, errors.Wrap(err, fmt.Sprintf("GetItem error for table %s key %+v", tableName, key))
	}
	if len(resp.Item) == 0 {
		return nil, &notFoundError{tableName: tableName, key: key}
	}
	return resp.Item, nil
}

func txGetOwner(item map[string]*dynamodb.AttributeValue) string {
	if item == nil {
		return ""
	}

	itemTxID := item[AttributeNameTxID]
	if itemTxID == nil || itemTxID.S == nil {
		return ""
	}
	return *itemTxID.S
}
