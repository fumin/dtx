/*
Package dtx provides transactions for AWS DynamoDB.
It is a port of the official Java library
https://github.com/awslabs/dynamodb-transactions
with slight modifications.

RunInTransaction wraps multiple operations in a single transaction:
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
In a transaction, Get items are isolated from other transactions,
and all operations either fail or succeed together.

This package requires several preparations to made,
including the creation of two extra tables,
and a background cleanup job to be run.
Use the functions VerifyOrCreateTransactionTable and VerifyOrCreateTransactionImagesTable to create the tables,
and invoke the Sweep function in the backgroud cleanup job.

There are several known caveats of this package:

* If you are using transactions on items in a table,
you should perform write operations only using this transaction library.
Failing to do so might cause transactions to become stuck permanently,
lost writes (either from transactions or from your writes),
or other undefined behavior.

* There can only be a single Put, Update, or Delete operation on an item in a transaction. Multiple Gets are fine.

* This package supports only the new UpdateExpression and ConditionalExpression syntaxes, and not the legacy ones such as AttributeUpdate and Expected.

* The only read isolation level supported is ReadCommitted.
Thus, for range queries, phantom reads are possible https://en.wikipedia.org/wiki/Isolation_(database_systems)#Phantom_reads .

* This package is not backwards compatible with the Java version,
due to slight differences in the internal logic, and the naming of
the internal fields.
*/
package dtx

import (
	"fmt"
	"strconv"
	"time"

	"github.com/fumin/dtx/backoff"
)

var (
	backoffDefaultBase        float64 = 2
	backoffDefaultUnit                = 200 * time.Millisecond
	backoffDefaultMaxWaitTime         = 10 * time.Second
)

// A DuplicateRequestError reports that more than one Put, Update, or Delete operation has been added to a transaction.
type DuplicateRequestError struct {
	req         txRequest
	existingReq txRequest
}

func (err *DuplicateRequestError) Error() string {
	return fmt.Sprintf("request %+v duplicates existing request %+v", err.req, err.existingReq)
}

// A TransactionCommittedError reports that a transaction has been committed,
// and is unable to take new operations.
type TransactionCommittedError struct {
	txItem *transactionItem
}

func (err *TransactionCommittedError) Error() string {
	return fmt.Sprintf("transaction commited %+v", err.txItem)
}

// A TransactionRolledBackError reports that a transaction has been rolled back.
// A rolled back is neither able to take new operations nor be committed.
type TransactionRolledBackError struct {
	txItem *transactionItem
}

func (err *TransactionRolledBackError) Error() string {
	return fmt.Sprintf("transaction rolledback %+v", err.txItem)
}

// A Retrier controls how much time a coordinator has to wait before the next retry, and when it should stop retrying.
type Retrier interface {
	// Reset prepares a new series of retries.
	// For example, a Retrier that limits the maximum number of retries to some number might reset its counter to 0.
	Reset()

	// Wait is called by a coordinator before every retry attempt.
	// If Wait returns true, a retry is carried out; if false, the retry is halted.
	Wait() bool
}

func newJitterExpBackoff() *backoff.JitterExp {
	var base float64 = 2
	unit := 200 * time.Millisecond
	maxWaitTime := 10 * time.Second
	r := backoff.NewJitterExp(base, unit, maxWaitTime)
	return r
}

func timeToStr(t time.Time) string {
	nano := t.UnixNano()
	milli := nano / 1e6
	n := fmt.Sprintf("%.3f", float64(milli)/1000)
	return n
}

func strToTime(s string) (time.Time, error) {
	dt, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return time.Unix(0, 0), fmt.Errorf("ParseFloat %s %v", s, err)
	}
	sec := int64(dt)
	nsec := int64((dt - float64(sec)) * 1e9)
	t := time.Unix(sec, nsec)
	return t, nil
}
