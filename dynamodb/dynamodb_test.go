package dynamodb

import (
	"fmt"
	"testing"
)

func TestUpdateExpression(t *testing.T) {
	exprStr := "SET list[0] = :val1 REMOVE #m.nestedField1, #m.nestedField2 ADD aNumber :val2, anotherNumber :val3 DELETE aSet :val4"
	expr := ParseUpdateExpr(&exprStr)

	if err := strSliceEqual(expr.Set, []string{"list[0] = :val1"}); err != nil {
		t.Fatalf("%v", err)
	}
	if err := strSliceEqual(expr.Remove, []string{"#m.nestedField1", "#m.nestedField2"}); err != nil {
		t.Fatalf("%v", err)
	}
	if err := strSliceEqual(expr.Add, []string{"aNumber :val2", "anotherNumber :val3"}); err != nil {
		t.Fatalf("%v", err)
	}
	if err := strSliceEqual(expr.Delete, []string{"aSet :val4"}); err != nil {
		t.Fatalf("%v", err)
	}

	if expr.String() != exprStr {
		t.Fatalf("%s != %s", expr.String(), exprStr)
	}
}

func strSliceEqual(a, b []string) error {
	if len(a) != len(b) {
		return fmt.Errorf("different lengths %d %d", len(a), len(b))
	}

	for i, ae := range a {
		be := b[i]
		if ae != be {
			return fmt.Errorf("strings not equal at position %d %s %s", i, ae, be)
		}
	}
	return nil
}
