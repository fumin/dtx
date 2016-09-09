package dtx

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/pkg/errors"

	"github.com/fumin/dtx/omitemptyjson"
)

const (
	requestTypeGet    = "G"
	requestTypeUpdate = "U"
	requestTypeDelete = "D"
	requestTypePut    = "P"
)

var (
	// ValidReturnValues are ReturnValues that are currently supported by this package.
	ValidReturnValues = map[string]struct{}{
		"ALL_NEW": struct{}{},
		// "ALL_OLD": struct{}{},
		// "NONE": struct{}{},
	}
)

type txRequest interface {
	getRid() int
	setRid(rid int)
	getTableName() string

	getKey() map[string]*dynamodb.AttributeValue
	getImmutableKey() string

	getReturnValues() string
	doValidate(txID string, txManager *TransactionManager) error

	marshal() ([]byte, error)
	unmarshal([]byte) error
}

type marshaledRequest struct {
	Type string           `json:"T"`
	Data *json.RawMessage `json:"D"`
}

func marshalRequest(req txRequest) ([]byte, error) {
	mr := marshaledRequest{}
	if _, ok := req.(*getItemRequest); ok {
		mr.Type = requestTypeGet
	} else if _, ok := req.(*updateItemRequest); ok {
		mr.Type = requestTypeUpdate
	} else if _, ok := req.(*deleteItemRequest); ok {
		mr.Type = requestTypeDelete
	} else if _, ok := req.(*putItemRequest); ok {
		mr.Type = requestTypePut
	} else {
		return nil, fmt.Errorf("unknown request type %+v", req)
	}

	b, err := req.marshal()
	if err != nil {
		return nil, errors.Wrap(err, "omitemptyjson.Marshal")
	}
	jrm := json.RawMessage(b)
	mr.Data = &jrm

	mrb, err := omitemptyjson.Marshal(mr)
	if err != nil {
		return nil, errors.Wrap(err, "json.Marshal")
	}
	return mrb, nil
}

func unmarshalRequest(b []byte) (txRequest, error) {
	mr := marshaledRequest{}
	if err := json.Unmarshal(b, &mr); err != nil {
		return nil, errors.Wrap(err, "json.Unmarshal")
	}
	var req txRequest
	switch mr.Type {
	case requestTypeGet:
		req = &getItemRequest{}
	case requestTypeUpdate:
		req = &updateItemRequest{}
	case requestTypeDelete:
		req = &deleteItemRequest{}
	case requestTypePut:
		req = &putItemRequest{}
	default:
		return nil, fmt.Errorf("unknown type %s", mr.Type)
	}
	if err := req.unmarshal(*mr.Data); err != nil {
		return nil, errors.Wrap(err, "unmarshal")
	}
	return req, nil
}

func copyRequest(req txRequest) (txRequest, error) {
	b, err := marshalRequest(req)
	if err != nil {
		return nil, errors.Wrap(err, "marshalRequest")
	}
	newReq, err := unmarshalRequest(b)
	if err != nil {
		return nil, errors.Wrap(err, "unmarshalRequest")
	}
	return newReq, nil
}

func validateRequest(req txRequest, txID string, txManager *TransactionManager) error {
	if req.getTableName() == "" {
		return fmt.Errorf("tableName must not be null %s", txID)
	}
	if len(req.getKey()) == 0 {
		return fmt.Errorf("the request key cannot be empty")
	}
	if req.getReturnValues() != "" {
		if _, ok := ValidReturnValues[req.getReturnValues()]; !ok {
			return fmt.Errorf("unsupported ReturnValues %s", req.getReturnValues())
		}
	}
	if err := req.doValidate(txID, txManager); err != nil {
		return errors.Wrap(err, "doValidate")
	}
	return nil
}

// All Fields need to be public for marshaling to work.
type getItemRequest struct {
	Rid   int                    `json:"R"`
	Input *dynamodb.GetItemInput `json:"I"`

	ImmutableKey string `json:"-"`
}

func newGetItemRequest(input *dynamodb.GetItemInput) (*getItemRequest, error) {
	req := &getItemRequest{}
	req.Input = input

	imtKey, err := toImmutableKey(req.Input.Key)
	if err != nil {
		return nil, errors.Wrap(err, "toImmutableKey")
	}
	req.ImmutableKey = imtKey

	return req, nil
}

func (req *getItemRequest) marshal() ([]byte, error) {
	return omitemptyjson.Marshal(req)
}

func (req *getItemRequest) unmarshal(b []byte) error {
	if err := json.Unmarshal(b, req); err != nil {
		return errors.Wrap(err, "json.Unmarshal")
	}

	imtKey, err := toImmutableKey(req.Input.Key)
	if err != nil {
		return errors.Wrap(err, "toImmutableKey")
	}
	req.ImmutableKey = imtKey

	return nil
}

func (req *getItemRequest) getRid() int {
	return req.Rid
}

func (req *getItemRequest) setRid(rid int) {
	req.Rid = rid
}

func (req *getItemRequest) getTableName() string {
	if req.Input.TableName == nil {
		return ""
	}
	return *req.Input.TableName
}

func (req *getItemRequest) getKey() map[string]*dynamodb.AttributeValue {
	return req.Input.Key
}

func (req *getItemRequest) getImmutableKey() string {
	return req.ImmutableKey
}

func (req *getItemRequest) getReturnValues() string {
	return ""
}

func (req *getItemRequest) doValidate(txID string, txManager *TransactionManager) error {
	if err := validateKeyAttributes(req, req.Input.Key, txID, txManager); err != nil {
		return errors.Wrap(err, "validateKeyAttributes")
	}
	if err := validateProjectionExpressionAttributes(req, req.Input.ProjectionExpression, txID, txManager); err != nil {
		return errors.Wrap(err, "validateProjectionExpressionAttributes")
	}
	if err := validateAttributes(req, req.Input.AttributesToGet, txID, txManager); err != nil {
		return errors.Wrap(err, "validateAttributes")
	}
	return nil
}

// All Fields need to be public for marshaling to work.
type updateItemRequest struct {
	Rid   int                       `json:"R"`
	Input *dynamodb.UpdateItemInput `json:"I"`

	ImmutableKey string `json:"-"`
}

func newUpdateItemRequest(input *dynamodb.UpdateItemInput) (*updateItemRequest, error) {
	req := &updateItemRequest{}
	req.Input = input

	imtKey, err := toImmutableKey(req.Input.Key)
	if err != nil {
		return nil, errors.Wrap(err, "toImmutableKey")
	}
	req.ImmutableKey = imtKey

	return req, nil
}

func (req *updateItemRequest) marshal() ([]byte, error) {
	return omitemptyjson.Marshal(req)
}

func (req *updateItemRequest) unmarshal(b []byte) error {
	if err := json.Unmarshal(b, req); err != nil {
		return errors.Wrap(err, "json.Unmarshal")
	}

	imtKey, err := toImmutableKey(req.Input.Key)
	if err != nil {
		return errors.Wrap(err, "toImmutableKey")
	}
	req.ImmutableKey = imtKey

	return nil
}

func (req *updateItemRequest) getRid() int {
	return req.Rid
}

func (req *updateItemRequest) setRid(rid int) {
	req.Rid = rid
}

func (req *updateItemRequest) getTableName() string {
	if req.Input.TableName == nil {
		return ""
	}
	return *req.Input.TableName
}

func (req *updateItemRequest) getKey() map[string]*dynamodb.AttributeValue {
	return req.Input.Key
}

func (req *updateItemRequest) getImmutableKey() string {
	return req.ImmutableKey
}

func (req *updateItemRequest) getReturnValues() string {
	if req.Input.ReturnValues == nil {
		return ""
	}
	return *req.Input.ReturnValues
}

func (req *updateItemRequest) doValidate(txID string, txManager *TransactionManager) error {
	if err := validateKeyAttributes(req, req.Input.Key, txID, txManager); err != nil {
		return errors.Wrap(err, "validateKeyAttributes")
	}

	if req.Input.UpdateExpression != nil {
		splitted := strings.Split(*req.Input.UpdateExpression, " ")
		for _, s := range splitted {
			if strings.HasPrefix(s, TxAttrPrefix) {
				return fmt.Errorf(`update expression "%s" must not contain reserved attributes, txID: %s`, *req.Input.UpdateExpression, txID)
			}
		}
	}

	for _, v := range req.Input.ExpressionAttributeNames {
		if strings.HasPrefix(*v, TxAttrPrefix) {
			return fmt.Errorf(`expression attribute names %+v must not contain reserved attributes, txID: %s`, req.Input.ExpressionAttributeNames, txID)
		}
	}
	return nil
}

// All Fields need to be public for marshaling to work.
type deleteItemRequest struct {
	Rid   int                       `json:"R"`
	Input *dynamodb.DeleteItemInput `json:"I"`

	ImmutableKey string `json:"-"`
}

func newDeleteItemRequest(input *dynamodb.DeleteItemInput) (*deleteItemRequest, error) {
	req := &deleteItemRequest{}
	req.Input = input

	imtKey, err := toImmutableKey(req.Input.Key)
	if err != nil {
		return nil, errors.Wrap(err, "toImmutableKey")
	}
	req.ImmutableKey = imtKey

	return req, nil
}

func (req *deleteItemRequest) marshal() ([]byte, error) {
	return omitemptyjson.Marshal(req)
}

func (req *deleteItemRequest) unmarshal(b []byte) error {
	if err := json.Unmarshal(b, req); err != nil {
		return errors.Wrap(err, "json.Unmarshal")
	}

	imtKey, err := toImmutableKey(req.Input.Key)
	if err != nil {
		return errors.Wrap(err, "toImmutableKey")
	}
	req.ImmutableKey = imtKey

	return nil
}

func (req *deleteItemRequest) getRid() int {
	return req.Rid
}

func (req *deleteItemRequest) setRid(rid int) {
	req.Rid = rid
}

func (req *deleteItemRequest) getTableName() string {
	if req.Input.TableName == nil {
		return ""
	}
	return *req.Input.TableName
}

func (req *deleteItemRequest) getKey() map[string]*dynamodb.AttributeValue {
	return req.Input.Key
}

func (req *deleteItemRequest) getImmutableKey() string {
	return req.ImmutableKey
}

func (req *deleteItemRequest) getReturnValues() string {
	if req.Input.ReturnValues == nil {
		return ""
	}
	return *req.Input.ReturnValues
}

func (req *deleteItemRequest) doValidate(txID string, txManager *TransactionManager) error {
	if err := validateKeyAttributes(req, req.Input.Key, txID, txManager); err != nil {
		return errors.Wrap(err, "validateKeyAttributes")
	}
	return nil
}

// All Fields need to be public for marshaling to work.
type putItemRequest struct {
	Rid   int                    `json:"R"`
	Input *dynamodb.PutItemInput `json:"I"`

	Key          map[string]*dynamodb.AttributeValue `json:"K"`
	ImmutableKey string                              `json:"-"`
}

func newPutItemRequest(input *dynamodb.PutItemInput, manager *TransactionManager) (*putItemRequest, error) {
	req := &putItemRequest{}
	req.Input = input

	key, err := manager.createKeyMap(*input.TableName, input.Item)
	if err != nil {
		return nil, errors.Wrap(err, "createKeyMap")
	}
	req.Key = key

	imtKey, err := toImmutableKey(req.Key)
	if err != nil {
		return nil, errors.Wrap(err, "toImmutableKey")
	}
	req.ImmutableKey = imtKey

	return req, nil
}

func (req *putItemRequest) marshal() ([]byte, error) {
	return omitemptyjson.Marshal(req)
}

func (req *putItemRequest) unmarshal(b []byte) error {
	if err := json.Unmarshal(b, req); err != nil {
		return errors.Wrap(err, "json.Unmarshal")
	}

	imtKey, err := toImmutableKey(req.Key)
	if err != nil {
		return errors.Wrap(err, "toImmutableKey")
	}
	req.ImmutableKey = imtKey

	return nil
}

func (req *putItemRequest) getRid() int {
	return req.Rid
}

func (req *putItemRequest) setRid(rid int) {
	req.Rid = rid
}

func (req *putItemRequest) getTableName() string {
	if req.Input.TableName == nil {
		return ""
	}
	return *req.Input.TableName
}

func (req *putItemRequest) getKey() map[string]*dynamodb.AttributeValue {
	return req.Key
}

func (req *putItemRequest) getImmutableKey() string {
	return req.ImmutableKey
}

func (req *putItemRequest) getReturnValues() string {
	if req.Input.ReturnValues == nil {
		return ""
	}
	return *req.Input.ReturnValues
}

func (req *putItemRequest) doValidate(txID string, txManager *TransactionManager) error {
	if err := validateKeyAttributes(req, req.Input.Item, txID, txManager); err != nil {
		return errors.Wrap(err, "validateKeyAttributes")
	}
	return nil
}

func validateKeyAttributes(req txRequest, key map[string]*dynamodb.AttributeValue, txID string, txManager *TransactionManager) error {
	for k, _ := range key {
		if strings.HasPrefix(k, TxAttrPrefix) {
			return fmt.Errorf("Request must not contain the reserved attribute %s, txID: %s", k, txID)
		}
	}
	return nil
}

func validateProjectionExpressionAttributes(req txRequest, projectionExpression *string, txID string, txManager *TransactionManager) error {
	if projectionExpression == nil {
		return nil
	}
	splitted := strings.Split(*projectionExpression, ",")
	trimmed := make([]*string, 0, len(splitted))
	for _, s := range splitted {
		trimmed = append(trimmed, aws.String(strings.TrimSpace(s)))
	}

	return validateAttributes(req, trimmed, txID, txManager)
}

func validateAttributes(req txRequest, attrs []*string, txID string, txManager *TransactionManager) error {
	for _, s := range attrs {
		if strings.HasPrefix(*s, TxAttrPrefix) {
			return fmt.Errorf("Request must not contain the reserved attribute %s, txID: %s", *s, txID)
		}
	}
	return nil
}
