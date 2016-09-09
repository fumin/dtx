package dtx

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/golang/glog"
	"github.com/pkg/errors"
)

const (
	// TransactionItemStatePending indicates that a transaction is ready to take new operations.
	TransactionItemStatePending = "P"
	// TransactionItemStateCommitted indicates that a transaction has been committed, and all operations in the transaction have been applied.
	TransactionItemStateCommitted = "C"
	// TransactionItemStateRolledBack indicates that a transaction has been rolled back.
	TransactionItemStateRolledBack = "R"
)

type transactionItem struct {
	id          string
	txManager   *TransactionManager
	txItem      map[string]*dynamodb.AttributeValue
	version     int
	txKey       map[string]*dynamodb.AttributeValue
	requestsMap map[string]map[string]txRequest
}

func newTransactionItem(txID string, txManager *TransactionManager, insert bool) (*transactionItem, error) {
	item := &transactionItem{}
	item.requestsMap = make(map[string]map[string]txRequest)
	item.txManager = txManager
	item.id = txID
	item.txKey = make(map[string]*dynamodb.AttributeValue)
	item.txKey[AttributeNameTxID] = &dynamodb.AttributeValue{S: &txID}
	var err error
	if insert {
		item.txItem, err = txItemInsert(item)
		if err != nil {
			return nil, errors.Wrap(err, "txItemInsert")
		}
	} else {
		item.txItem, err = txItemGet(item)
		if err != nil {
			return nil, errors.Wrap(err, "txItemGet")
		}
	}

	version, err := unmarshalItemVersion(item.txItem)
	if err != nil {
		return nil, errors.Wrap(err, "unmarshalItemVersion")
	}
	item.version = version

	reqsMap, err := txItemLoadRequests(item.txManager, item.txItem)
	if err != nil {
		return nil, errors.Wrap(err, "txItemLoadRequests")
	}
	item.requestsMap = reqsMap

	return item, nil
}

func newTransactionItemByItem(txItem map[string]*dynamodb.AttributeValue, txManager *TransactionManager) (*transactionItem, error) {
	item := &transactionItem{}
	item.requestsMap = make(map[string]map[string]txRequest)
	item.txManager = txManager
	item.txItem = txItem
	if !isTransactionItem(item.txItem) {
		return nil, fmt.Errorf("not a transaction item %+v", txItem)
	}
	item.id = *item.txItem[AttributeNameTxID].S
	item.txKey = make(map[string]*dynamodb.AttributeValue)
	item.txKey[AttributeNameTxID] = &dynamodb.AttributeValue{S: &item.id}

	version, err := unmarshalItemVersion(item.txItem)
	if err != nil {
		return nil, errors.Wrap(err, "unmarshalItemVersion")
	}
	item.version = version

	reqsMap, err := txItemLoadRequests(item.txManager, item.txItem)
	if err != nil {
		return nil, errors.Wrap(err, "txItemLoadRequests")
	}
	item.requestsMap = reqsMap

	return item, nil
}

func (txItem *transactionItem) saveItemImage(item map[string]*dynamodb.AttributeValue, rid int) error {
	if _, ok := item[AttributeNameApplied]; ok {
		return fmt.Errorf("the transaction has already applied this item image, it should not be saving over the item image with it %s", txItem.id)
	}
	txIDVal, ok := item[AttributeNameTxID]
	if !ok || txIDVal.S == nil {
		return fmt.Errorf("items in transactions should contain the attribute named %s", AttributeNameTxID)
	}
	if *txIDVal.S != txItem.id {
		return fmt.Errorf("cannot save item with TX %s in TX %s", *txIDVal.S, txItem.id)
	}
	if _, ok := item[AttributeNameImageID]; ok {
		return fmt.Errorf("items in transactions may not contain the attribute named %s", AttributeNameImageID)
	}

	// Avoid modifying item before we are sure that our operation would succeed.
	copiedItem := make(map[string]*dynamodb.AttributeValue)
	for k, v := range item {
		copiedItem[k] = v
	}
	copiedItem[AttributeNameImageID] = &dynamodb.AttributeValue{S: aws.String(fmt.Sprintf("%s#%d", txItem.id, rid))}

	// Don't save over the already saved item.  Prevents us from saving the applied image instead of the previous image in the case of a re-drive.
	eav := make(map[string]*dynamodb.AttributeValue)
	attrEqAnds := make([]string, 0, len(copiedItem))
	for k, v := range copiedItem {
		// The date might change upon re-drives, since Transaction.lockItem updates it.
		if k == AttributeNameDate {
			continue
		}
		exprName := fmt.Sprintf(":%s", k)
		eav[exprName] = v
		attrEqAnds = append(attrEqAnds, fmt.Sprintf("%s = %s", k, exprName))
	}
	condExpr := fmt.Sprintf("attribute_not_exists(%s) OR (%s)", AttributeNameImageID, strings.Join(attrEqAnds, " AND "))

	input := &dynamodb.PutItemInput{
		TableName:                 &txItem.txManager.itemImageTableName,
		ExpressionAttributeValues: eav,
		ConditionExpression:       &condExpr,
		Item:                      copiedItem,
	}
	_, err := txItem.txManager.client.PutItem(input)
	if err != nil {
		return errors.Wrap(err, "PutItem")
	}

	item[AttributeNameImageID] = copiedItem[AttributeNameImageID]
	return nil
}

func (txItem *transactionItem) loadItemImage(rid int) (map[string]*dynamodb.AttributeValue, error) {
	key := make(map[string]*dynamodb.AttributeValue)
	key[AttributeNameImageID] = &dynamodb.AttributeValue{S: aws.String(fmt.Sprintf("%s#%d", txItem.id, rid))}
	input := &dynamodb.GetItemInput{
		TableName:      &txItem.txManager.itemImageTableName,
		Key:            key,
		ConsistentRead: aws.Bool(true),
	}
	resp, err := txItem.txManager.client.GetItem(input)
	if err != nil {
		return nil, errors.Wrap(err, "GetItem")
	}

	item := resp.Item
	if item != nil {
		delete(item, AttributeNameImageID)
	}
	return item, nil
}

func (txItem *transactionItem) deleteItemImage(rid int) error {
	key := make(map[string]*dynamodb.AttributeValue)
	key[AttributeNameImageID] = &dynamodb.AttributeValue{S: aws.String(fmt.Sprintf("%s#%d", txItem.id, rid))}
	input := &dynamodb.DeleteItemInput{
		TableName: &txItem.txManager.itemImageTableName,
		Key:       key,
	}
	_, err := txItem.txManager.client.DeleteItem(input)
	if err != nil {
		return errors.Wrap(err, "DeleteItem")
	}
	return nil
}

func (txItem *transactionItem) finish(targetState string) error {
	if targetState != TransactionItemStateRolledBack && targetState != TransactionItemStateCommitted {
		return fmt.Errorf("illegal state in finish: %s", targetState)
	}

	eav := make(map[string]*dynamodb.AttributeValue)
	eav[":state"] = &dynamodb.AttributeValue{S: aws.String(TransactionItemStatePending)}
	eav[":version"] = &dynamodb.AttributeValue{N: aws.String(fmt.Sprintf("%d", txItem.version))}
	conditionExpression := fmt.Sprintf("%s = :state AND %s = :version", AttributeNameState, AttributeNameVersion)

	eav[":targetState"] = &dynamodb.AttributeValue{S: &targetState}
	eav[":date"] = txItem.txManager.getCurrentTimeAttribute()
	updateExpr := fmt.Sprintf("SET %s = :targetState, %s = :date", AttributeNameState, AttributeNameDate)
	input := &dynamodb.UpdateItemInput{
		TableName: &txItem.txManager.transactionTableName,
		Key:       txItem.txKey,
		ExpressionAttributeValues: eav,
		ConditionExpression:       &conditionExpression,
		UpdateExpression:          &updateExpr,
		ReturnValues:              aws.String("ALL_NEW"),
	}
	resp, err := txItem.txManager.client.UpdateItem(input)
	if err != nil {
		return errors.Wrap(err, "UpdateItem")
	}
	if len(resp.Attributes) == 0 {
		return fmt.Errorf("unexpected null tx item after committing %s %+v", targetState, resp)
	}
	txItem.txItem = resp.Attributes
	return nil
}

func (txItem *transactionItem) delete() error {
	input := &dynamodb.DeleteItemInput{
		TableName: &txItem.txManager.transactionTableName,
		Key:       txItem.txKey,
	}
	_, err := txItem.txManager.client.DeleteItem(input)
	if err != nil {
		return errors.Wrap(err, "DeleteItem")
	}
	return nil
}

func (txItem *transactionItem) getState() (string, error) {
	stateVal := txItem.txItem[AttributeNameState]
	if stateVal == nil || stateVal.S == nil {
		return "", fmt.Errorf("no state")
	}
	txState := *stateVal.S

	if txState == TransactionItemStateCommitted {
		return TransactionItemStateCommitted, nil
	} else if txState == TransactionItemStateRolledBack {
		return TransactionItemStateRolledBack, nil
	} else if txState == TransactionItemStatePending {
		return TransactionItemStatePending, nil
	} else {
		glog.Warningf("unrecognized transaction state: %s", txState)
		return "", fmt.Errorf("unrecognized transaction state: %s", txState)
	}
}

func (txItem *transactionItem) addRequest(callerRequest txRequest) error {
	// Ensure the request is unique
	_, err := isReqDuplicate(callerRequest, txItem.requestsMap)
	if err != nil {
		return errors.Wrap(err, "isReqDuplicate")
	}

	// Marshal the request with the new incremented version, without modifying it.
	// This is achieved by setting the new incremented version only a cloned copy.
	clonedReq, err := copyRequest(callerRequest)
	if err != nil {
		return errors.Wrap(err, "copyRequest")
	}
	clonedReq.setRid(txItem.version + 1)
	requestBytes, err := marshalRequest(clonedReq)
	if err != nil {
		return errors.Wrap(err, "marshalRequest")
	}

	// Write request to transaction item
	eav := make(map[string]*dynamodb.AttributeValue)
	// UpdateExpression
	updateExpr := fmt.Sprintf("ADD %s :req, %s :versionIncr SET %s = :date", AttributeNameRequests, AttributeNameVersion, AttributeNameDate)
	eav[":req"] = &dynamodb.AttributeValue{BS: [][]byte{requestBytes}}
	eav[":versionIncr"] = &dynamodb.AttributeValue{N: aws.String("1")}
	eav[":date"] = txItem.txManager.getCurrentTimeAttribute()

	// ConditionExpression
	eav[":state"] = &dynamodb.AttributeValue{S: aws.String(TransactionItemStatePending)}
	eav[":expectedVersion"] = &dynamodb.AttributeValue{N: aws.String(fmt.Sprintf("%d", txItem.version))}
	conditionExpression := fmt.Sprintf("%s = :state AND %s = :expectedVersion", AttributeNameState, AttributeNameVersion)

	input := &dynamodb.UpdateItemInput{
		TableName: &txItem.txManager.transactionTableName,
		Key:       txItem.txKey,
		ExpressionAttributeValues: eav,
		ConditionExpression:       &conditionExpression,
		UpdateExpression:          &updateExpr,
		ReturnValues:              aws.String("ALL_NEW"),
	}
	resp, err := txItem.txManager.client.UpdateItem(input)
	if err != nil {
		return errors.Wrap(err, "UpdateItem")
	}

	newVersion, err := unmarshalItemVersion(resp.Attributes)
	if err != nil {
		return errors.Wrap(err, "unmarshalItemVersion")
	}
	if newVersion != txItem.version+1 {
		return fmt.Errorf("unexpected version number from update result %d %d", newVersion, txItem.version)
	}

	reqsMap, err := txItemLoadRequests(txItem.txManager, resp.Attributes)
	if err != nil {
		return errors.Wrap(err, "txItemLoadRequests")
	}

	// All operations have succeeded, update the internal fields.
	txItem.txItem = resp.Attributes
	txItem.requestsMap = reqsMap
	txItem.version = newVersion
	callerRequest.setRid(txItem.version)
	return nil
}

func (txItem *transactionItem) getRequestsSorted() []txRequest {
	tableNames := make([]string, 0, len(txItem.requestsMap))
	for tbn, _ := range txItem.requestsMap {
		tableNames = append(tableNames, tbn)
	}
	sort.Strings(tableNames)

	reqs := make([]txRequest, 0, len(tableNames))
	for _, tbn := range tableNames {
		tbReqs := txItem.requestsMap[tbn]
		primaryKeys := make([]string, 0, len(tbReqs))
		for pk, _ := range tbReqs {
			primaryKeys = append(primaryKeys, pk)
		}
		sort.Strings(primaryKeys)

		for _, pk := range primaryKeys {
			reqs = append(reqs, tbReqs[pk])
		}
	}

	return reqs
}

func isTransactionItem(txItem map[string]*dynamodb.AttributeValue) bool {
	if txItem == nil {
		return false
	}
	av, ok := txItem[AttributeNameTxID]
	if !ok {
		return false
	}
	if av.S == nil {
		return false
	}
	return true
}

func txItemInsert(txItem *transactionItem) (map[string]*dynamodb.AttributeValue, error) {
	item := make(map[string]*dynamodb.AttributeValue)
	item[AttributeNameState] = &dynamodb.AttributeValue{S: aws.String(TransactionItemStatePending)}
	item[AttributeNameVersion] = &dynamodb.AttributeValue{N: aws.String("0")}
	item[AttributeNameDate] = txItem.txManager.getCurrentTimeAttribute()
	for k, v := range txItem.txKey {
		item[k] = v
	}

	condExpr := fmt.Sprintf("attribute_not_exists(%s) and attribute_not_exists(%s)", AttributeNameTxID, AttributeNameState)

	input := &dynamodb.PutItemInput{
		TableName:           &txItem.txManager.transactionTableName,
		Item:                item,
		ConditionExpression: &condExpr,
	}
	_, err := txItem.txManager.client.PutItem(input)
	if err != nil {
		return nil, errors.Wrap(err, "PutItem")
	}
	return item, nil
}

func txItemGet(txItem *transactionItem) (map[string]*dynamodb.AttributeValue, error) {
	input := &dynamodb.GetItemInput{
		TableName:      &txItem.txManager.transactionTableName,
		Key:            txItem.txKey,
		ConsistentRead: aws.Bool(true),
	}
	resp, err := txItem.txManager.client.GetItem(input)
	if err != nil {
		return nil, errors.Wrap(err, "GetItem")
	}
	return resp.Item, nil
}

func txItemLoadRequests(manager *TransactionManager, attrs map[string]*dynamodb.AttributeValue) (map[string]map[string]txRequest, error) {
	requestsVal := attrs[AttributeNameRequests]
	rawRequests := [][]byte{}
	if requestsVal != nil && requestsVal.BS != nil {
		rawRequests = requestsVal.BS
	}

	reqsMap := make(map[string]map[string]txRequest)
	for _, rr := range rawRequests {
		req, err := unmarshalRequest(rr)
		if err != nil {
			return nil, errors.Wrap(err, "unmarshalRequest")
		}
		existingReq, err := isReqDuplicate(req, reqsMap)
		if err != nil {
			return nil, errors.Wrap(err, "isReqDuplicate")
		}

		if existingReq != nil {
			if _, ok := existingReq.(*getItemRequest); !ok {
				continue
			}
		}
		reqsMap[req.getTableName()][req.getImmutableKey()] = req
	}

	return reqsMap, nil
}

func getReqInMap(tableName string, immutableKey string, reqsMap map[string]map[string]txRequest) txRequest {
	pkToRequestMap := reqsMap[tableName]
	if pkToRequestMap == nil {
		pkToRequestMap = make(map[string]txRequest)
		reqsMap[tableName] = pkToRequestMap
	}

	return pkToRequestMap[immutableKey]
}

func isReqDuplicate(req txRequest, requestsMap map[string]map[string]txRequest) (txRequest, error) {
	existingRequest := getReqInMap(req.getTableName(), req.getImmutableKey(), requestsMap)
	if existingRequest != nil {
		_, isReqGet := req.(*getItemRequest)
		_, isExistingReqGet := existingRequest.(*getItemRequest)
		if !isReqGet && !isExistingReqGet {
			return existingRequest, &DuplicateRequestError{req: req, existingReq: existingRequest}
		}
	}

	return existingRequest, nil
}

func unmarshalItemVersion(item map[string]*dynamodb.AttributeValue) (int, error) {
	txVersionVal := item[AttributeNameVersion]
	if txVersionVal == nil || txVersionVal.N == nil {
		return -1, fmt.Errorf("Version number is not present in TX record")
	}
	version, err := strconv.Atoi(*txVersionVal.N)
	if err != nil {
		return -1, fmt.Errorf("Version number is not present in TX record")
	}
	return version, nil
}

func lastUpdateTime(txAttrs map[string]*dynamodb.AttributeValue) (time.Time, error) {
	requestsVal := txAttrs[AttributeNameDate]
	if requestsVal == nil || requestsVal.N == nil {
		return time.Unix(0, 0), fmt.Errorf("no date attribute %+v", txAttrs)
	}

	t, err := strToTime(*requestsVal.N)
	if err != nil {
		return time.Unix(0, 0), errors.Wrap(err, "strToTime")
	}
	return t, nil
}
