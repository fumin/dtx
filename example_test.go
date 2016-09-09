package dtx_test

import (
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/pkg/errors"

	"github.com/fumin/dtx"
)

const (
	// The transaction table
	txTableName = "txTableName"
	// The transaction item image table
	txImageTableName = "txImageTableName"

	// The table containing the balances of users.
	// It's HASH key is the ID of a user.
	// balanceAttrName is the numeric field that contains the balance of the user.
	balanceTableName        = "balanceTableName"
	balanceTableHashKeyAttr = "balanceTableHashKeyAttr"
	balanceAttrName         = "balanceAttrName"

	// The table of the account books of users.
	// It contains every change that has been made to a user's balance.
	// It's HASH key is the ID of a user, and its RANGE key is the time of the change.
	// accountBookAmountAttrName is the numeric field that contains the amount of the change.
	// accountBookDescAttrName is the string field that contains the description of the change.
	accountBookTableName         = "accountBookTableName"
	accountBookTableHashKeyAttr  = "accountBookTableHashKeyAttr"
	accountBookTableRangeKeyAttr = "accountBookTableRangeKeyAttr"
	accountBookAmountAttrName    = "accountBookAmountAttrName"
	accountBookDescAttrName      = "accountBookDescAttrName"

	awsEndpoint        = "http://127.0.0.1:8000"
	awsRegion          = "us-east-1"
	awsAccessKeyID     = "dtxtestAWSAccessKeyID"
	awsSecretAccessKey = "dtxtestAWSSecretAccessKey"
	awsSessionToken    = "dtxtestAWSSessionToken"
)

var (
	sess = session.Must(session.NewSession(&aws.Config{
		Endpoint:    aws.String(awsEndpoint),
		Region:      aws.String(awsRegion),
		Credentials: credentials.NewStaticCredentials(awsAccessKeyID, awsSecretAccessKey, awsSessionToken),
	}))
	client  = dynamodb.New(sess)
	manager = dtx.NewTransactionManager(client, txTableName, txImageTableName)
)

// This example demonstrates the use of transactions in enforcing a rule in a bank which states that all accounts must have a positive balance.
// Let's say Bob initially has $50 in his account.
// Without transactions, two concurrent attempts to withdraw 40 dollars from Bob's account might result in a -$30 balance,
// due to the fact that the check of the second attempt might happen before the withdrawal of the first.
// With transactions, since reads are isolated from writes of other transactions, this kind of error is prevented.
func Example_readIsolation() {
	setup()
	bob := "bob"
	setUserBalance(bob, 50)

	// Simulate the first withdrawal attempt, which pauses before doing the actual deduction from Bob's balance,
	// allowing the second withdrawal attempt to also see that Bob still has $50 in his balance.
	var deductAmount float64 = -40
	t1CheckFinished := make(chan struct{})
	t1StartWithdrawal := make(chan struct{})
	t1Err := make(chan error)
	go func() {
		t1Err <- manager.RunInTransaction(func(tx *dtx.Transaction) error {
			balance, err := getBalance(bob, tx)
			if err != nil {
				return err
			}
			fmt.Printf("transaction 1 sees balance: $%.0f\n", balance)

			t1CheckFinished <- struct{}{}
			<-t1StartWithdrawal

			if err := incrementBalance(bob, deductAmount, tx); err != nil {
				return err
			}
			fmt.Printf("transaction 1 deducted $%.0f\n", -deductAmount)
			return nil
		})
	}()

	// Simulate the second withdrawal attempt, which rolls back the first withdrawal attempt,
	// and succeeds in deducted $40 from Bob's account.
	<-t1CheckFinished
	manager.RunInTransaction(func(tx *dtx.Transaction) error {
		balance, _ := getBalance(bob, tx)
		fmt.Printf("transaction 2 sees balance: $%.0f\n", balance)
		incrementBalance(bob, deductAmount, tx)
		fmt.Printf("transaction 2 deducted $%.0f\n", -deductAmount)
		return nil
	})
	t1StartWithdrawal <- struct{}{}

	// Check that the first attempt is rolled back.
	err := <-t1Err
	if _, ok := errors.Cause(err).(*dtx.TransactionRolledBackError); ok {
		fmt.Printf("transaction 1 rolled back\n")
	}

	balance := queryBalance(bob)
	fmt.Printf("final balance: $%.0f\n", balance)

	// Output:
	// transaction 1 sees balance: $50
	// transaction 2 sees balance: $50
	// transaction 2 deducted $40
	// transaction 1 rolled back
	// final balance: $10
}

// This example demonstrates that all operations in a transaction always fail of succeed together.
// Imagine a bank that wants to enforce a rule that all changes made to an account must be accompanied by an insertion of a record in an account book.
// Without transactions, a machine failure might occur after a change to the balance is made, but before the corresponding record has been created, resulting in violations of the rule.
// With transactions, the change to the balance is rolled back, and data integrity is maintained.
func Example_atomic() {
	setup()
	bob := "bob"
	setUserBalance(bob, 100)
	balance := queryBalance(bob)
	fmt.Printf("initial balance: $%.0f\n", balance)

	// Simulate a machine failure between the deduction from the balance and the creation of a record.
	t1Ended := make(chan struct{})
	go func() {
		defer func() {
			if r := recover(); r != nil {
				fmt.Printf("%v\n", r)
			}
			close(t1Ended)
		}()
		manager.RunInTransaction(func(tx *dtx.Transaction) error {
			var amount float64 = -10
			incrementBalance(bob, amount, tx)
			balance, _ := getBalance(bob, tx)
			fmt.Printf("balance after deduction of 1st transaction: $%.0f\n", balance)
			panic("machine 1 failed")
			addRecordToAccountBook(bob, amount, "transaction 1", tx)
			return nil
		})
	}()
	<-t1Ended
	balance = queryBalance(bob)
	fmt.Printf("balance after machine 1 crashed: $%.0f\n\n", balance)

	// Run a second transaction which is successful.
	manager.RunInTransaction(func(tx *dtx.Transaction) error {
		var amount float64 = -30
		incrementBalance(bob, amount, tx)
		addRecordToAccountBook(bob, amount, "transaction 2", tx)
		return nil
	})
	balance = queryBalance(bob)
	fmt.Printf("balance after 2nd transaction: $%.0f\n", balance)
	records := queryAccountBook(bob)
	fmt.Printf("records:\n")
	for _, rec := range records {
		fmt.Printf("\tamount: %.0f, description: \"%s\"\n", rec.Amount, rec.Description)
	}

	// Output:
	// initial balance: $100
	// balance after deduction of 1st transaction: $90
	// machine 1 failed
	// balance after machine 1 crashed: $100
	//
	// balance after 2nd transaction: $70
	// records:
	//	amount: -30, description: "transaction 2"
}

func setup() {
	deleteTable(client, txTableName)
	deleteTable(client, txImageTableName)
	deleteTable(client, balanceTableName)
	deleteTable(client, accountBookTableName)

	manager.VerifyOrCreateTransactionTable()
	manager.VerifyOrCreateTransactionImagesTable()
	createTable(client, balanceTableName, []*dynamodb.AttributeDefinition{{AttributeName: aws.String(balanceTableHashKeyAttr), AttributeType: aws.String("S")}}, []*dynamodb.KeySchemaElement{{AttributeName: aws.String(balanceTableHashKeyAttr), KeyType: aws.String("HASH")}})
	createTable(client, accountBookTableName, []*dynamodb.AttributeDefinition{{AttributeName: aws.String(accountBookTableHashKeyAttr), AttributeType: aws.String("S")}, {AttributeName: aws.String(accountBookTableRangeKeyAttr), AttributeType: aws.String("N")}}, []*dynamodb.KeySchemaElement{{AttributeName: aws.String(accountBookTableHashKeyAttr), KeyType: aws.String("HASH")}, {AttributeName: aws.String(accountBookTableRangeKeyAttr), KeyType: aws.String("RANGE")}})
}

// getBalance returns the balance of a user.
func getBalance(user string, tx *dtx.Transaction) (float64, error) {
	key := make(map[string]*dynamodb.AttributeValue)
	key[balanceTableHashKeyAttr] = &dynamodb.AttributeValue{S: aws.String(user)}
	input := &dynamodb.GetItemInput{
		TableName: aws.String(balanceTableName),
		Key:       key,
	}
	output, err := tx.GetItem(input)
	if err != nil {
		return -1, err
	}
	balance, err := strconv.ParseFloat(*output.Item[balanceAttrName].N, 64)
	if err != nil {
		return -1, err
	}
	return balance, nil
}

// incrementBalance deducts money from a user's balance.
func incrementBalance(user string, amount float64, tx *dtx.Transaction) error {
	key := make(map[string]*dynamodb.AttributeValue)
	key[balanceTableHashKeyAttr] = &dynamodb.AttributeValue{S: aws.String(user)}
	eav := make(map[string]*dynamodb.AttributeValue)
	eav[":balance"] = &dynamodb.AttributeValue{N: aws.String(fmt.Sprintf("%f", amount))}
	updateExpr := fmt.Sprintf("SET %s = %s + %s", balanceAttrName, balanceAttrName, ":balance")
	input := &dynamodb.UpdateItemInput{
		TableName: aws.String(balanceTableName),
		Key:       key,
		ExpressionAttributeValues: eav,
		UpdateExpression:          aws.String(updateExpr),
	}
	_, err := tx.UpdateItem(input)
	return err
}

// addRecordToAccountBook adds a record to a user's account book.
func addRecordToAccountBook(user string, amount float64, desc string, tx *dtx.Transaction) error {
	item := make(map[string]*dynamodb.AttributeValue)
	item[accountBookTableHashKeyAttr] = &dynamodb.AttributeValue{S: aws.String(user)}
	item[accountBookTableRangeKeyAttr] = &dynamodb.AttributeValue{N: aws.String(fmt.Sprintf("%d", time.Now().Unix()))}
	item[accountBookAmountAttrName] = &dynamodb.AttributeValue{N: aws.String(fmt.Sprintf("%f", amount))}
	item[accountBookDescAttrName] = &dynamodb.AttributeValue{S: aws.String(desc)}
	input := &dynamodb.PutItemInput{
		TableName: aws.String(accountBookTableName),
		Item:      item,
	}
	_, err := tx.PutItem(input)
	return err
}

// setUserBalance sets a user's balance.
func setUserBalance(user string, balance float64) {
	item := make(map[string]*dynamodb.AttributeValue)
	item[balanceTableHashKeyAttr] = &dynamodb.AttributeValue{S: aws.String(user)}
	item[balanceAttrName] = &dynamodb.AttributeValue{N: aws.String(fmt.Sprintf("%f", balance))}
	input := &dynamodb.PutItemInput{
		TableName: aws.String(balanceTableName),
		Item:      item,
	}
	err := manager.RunInTransaction(func(tx *dtx.Transaction) error {
		// We do not need to check errors, as the transaction will be rolled back whenever an operation fails.
		// Moreover, the error will be returned by RunInTransaction.
		tx.PutItem(input)
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
}

func queryBalance(user string) float64 {
	eav := make(map[string]*dynamodb.AttributeValue)
	eav[":user"] = &dynamodb.AttributeValue{S: aws.String(user)}
	input := &dynamodb.QueryInput{
		TableName:                 aws.String(balanceTableName),
		ExpressionAttributeValues: eav,
		KeyConditionExpression:    aws.String(fmt.Sprintf("%s = :user", balanceTableHashKeyAttr)),
	}
	output, err := manager.Query(input)
	if err != nil {
		log.Fatal(err)
	}
	balance, err := strconv.ParseFloat(*output.Items[0][balanceAttrName].N, 64)
	if err != nil {
		log.Fatal(err)
	}
	return balance
}

type record struct {
	User        string  `dynamodbav:"accountBookTableHashKeyAttr"`
	Time        int     `dynamodbav:"accountBookTableRangeKeyAttr"`
	Amount      float64 `dynamodbav:"accountBookAmountAttrName"`
	Description string  `dynamodbav:"accountBookDescAttrName"`
}

func queryAccountBook(user string) []record {
	eav := make(map[string]*dynamodb.AttributeValue)
	eav[":user"] = &dynamodb.AttributeValue{S: aws.String(user)}
	input := &dynamodb.QueryInput{
		TableName:                 aws.String(accountBookTableName),
		ExpressionAttributeValues: eav,
		KeyConditionExpression:    aws.String(fmt.Sprintf("%s = :user", accountBookTableHashKeyAttr)),
		ScanIndexForward:          aws.Bool(false),
	}
	output, err := manager.Query(input)
	if err != nil {
		log.Fatal(err)
	}

	recs := make([]record, 0, len(output.Items))
	for _, item := range output.Items {
		r := record{}
		if err := dynamodbattribute.UnmarshalMap(item, &r); err != nil {
			log.Fatal(err)
		}
		recs = append(recs, r)
	}
	return recs
}

func createTable(client *dynamodb.DynamoDB, tableName string, attrDefs []*dynamodb.AttributeDefinition, keySchema []*dynamodb.KeySchemaElement) error {
	provTP := &dynamodb.ProvisionedThroughput{
		ReadCapacityUnits:  aws.Int64(1),
		WriteCapacityUnits: aws.Int64(1),
	}
	input := &dynamodb.CreateTableInput{
		TableName:             &tableName,
		KeySchema:             keySchema,
		AttributeDefinitions:  attrDefs,
		ProvisionedThroughput: provTP,
	}
	_, err := client.CreateTable(input)
	return err
}

func deleteTable(client *dynamodb.DynamoDB, tableName string) (*dynamodb.DeleteTableOutput, error) {
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
