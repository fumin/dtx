package dtx

import (
	"fmt"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/golang/glog"
	"github.com/pkg/errors"
)

func isItemCommitted(item map[string]*dynamodb.AttributeValue) bool {
	_, isTransient := item[AttributeNameTransient]
	if isTransient {
		return false
	}
	// If the item isn't applied, it doesn't matter if it's locked
	_, isApplied := item[AttributeNameApplied]
	if !isApplied {
		return true
	}
	// If the item isn't locked, return it
	_, hasTxID := item[AttributeNameTxID]
	if !hasTxID {
		return true
	}
	return false
}

func handleReadCommitted(item map[string]*dynamodb.AttributeValue, tableName string, manager *TransactionManager) (map[string]*dynamodb.AttributeValue, error) {
	if isItemCommitted(item) {
		return item, nil
	}

	ownerID := txGetOwner(item)
	txItem, err := newTransactionItem(ownerID, manager, false)
	if err != nil {
		return nil, errors.Wrap(err, "newTransactionItem")
	}
	state, err := txItem.getState()
	if err != nil {
		return nil, errors.Wrap(err, "getState")
	}
	if state == TransactionItemStateCommitted {
		return item, nil
	}

	key, err := manager.createKeyMap(tableName, item)
	if err != nil {
		return nil, errors.Wrap(err, "createKeyMap")
	}
	immutableKey, err := toImmutableKey(key)
	if err != nil {
		return nil, errors.Wrap(err, "toImmutableKey")
	}
	lockingReq := getReqInMap(tableName, immutableKey, txItem.requestsMap)
	if lockingReq == nil {
		errMsg := fmt.Sprintf("no locking request %s %s %+v", tableName, immutableKey, txItem)
		glog.Warningf(errMsg)
		return nil, fmt.Errorf(errMsg)
	}
	oldItem, err := txItem.loadItemImage(lockingReq.getRid())
	if err != nil {
		return nil, errors.Wrap(err, "loadItemImage")
	}
	return oldItem, nil
}
