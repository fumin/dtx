# dtx - DynamodbDB transactions in Go

Package dtx provides transactions for AWS DynamoDB. It is a port of the official Java library https://github.com/awslabs/dynamodb-transactions with slight modifications.

RunInTransaction wraps multiple operations in a single transaction:

```
err := manager.RunInTransaction(func(tx *Transaction) error {
	output, err := tx.GetItem(input)
	if err != nil {
		return err
	}
	balance := strconv.Atoi(*output.Item["balance"].N)
	if balance - deductedAmount < 0 {
		return fmt.Errorf("negative balance")
	}
	tx.UpdateItem(updateInput)  // deduct money
	tx.PutItem(putInput)  // add a record in the account book
})
```

In a transaction, Get items are isolated from other transactions, and all operations either fail or succeed together.
For more examples, please consult the documentation at https://godoc.org/github.com/fumin/dtx .

This package requires several preparations to made, including the creation of two extra tables, and a background cleanup job to be run. Use the functions VerifyOrCreateTransactionTable and VerifyOrCreateTransactionImagesTable to create the tables, and invoke the Sweep function in the backgroud cleanup job.

There are several known caveats of this package:

* If you are using transactions on items in a table, you should perform write operations only using this transaction library. Failing to do so might cause transactions to become stuck permanently, lost writes (either from transactions or from your writes), or other undefined behavior.

* There can only be a single Put, Update, or Delete operation on an item in a transaction. Multiple Gets are fine.

* This package supports only the new UpdateExpression and ConditionalExpression syntaxes, and not the legacy ones such as AttributeUpdate and Expected.

* The only read isolation level supported is ReadCommitted. Thus, for range queries, phantom reads are possible https://en.wikipedia.org/wiki/Isolation_(database_systems)#Phantom_reads .

* This package is not backwards compatible with the Java version, due to slight differences in the internal logic, and the naming of the internal fields.

## Differences from the Java library
* The prefix of the names of the internal fields, TxAttrPrefix, is different.
* The way operations inside a transaction are serialized are different.
This library opted for a more Go idiomatic way.

## Development and testing
To run the tests of this package, install the local version of DynamoDB on your machine following the instructions in
http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.html .
After installing local DynamoDB, run it on port 8000.
Finaly run `go test`.

To view the contents of the local DynamoDB after the tests have finished,
run the below command, provided you have the AWS command line installed:
```
AWS_ACCESS_KEY_ID=dtxtestAWSAccessKeyID AWS_SECRET_ACCESS_KEY= AWS_DEFAULT_REGION=us-east-1 aws dynamodb scan --table-name dtxIntegHashTableName --endpoint-url http://127.0.0.1:8000/
```
