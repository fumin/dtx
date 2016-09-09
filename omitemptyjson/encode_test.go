package omitemptyjson

import (
	"encoding/json"
	"testing"
)

type SomeType struct {
	A string
	B string
	C *string
	D []*string
	E map[string]struct{}
}

func TestMarshal(t *testing.T) {
	obj1 := SomeType{
		B: "asdf",
	}
	b, err := Marshal(obj1)
	if err != nil {
		t.Fatalf("%v", err)
	}
	if string(b) != `{"A":"","B":"asdf"}` {
		t.Fatalf("%s", b)
	}

	obj2 := SomeType{}
	if err := json.Unmarshal(b, &obj2); err != nil {
		t.Fatalf("%v", err)
	}

	if obj2.A != "" {
		t.Fatalf("%+v", obj2)
	}
	if obj2.B != "asdf" {
		t.Fatalf("%+v", obj2)
	}
	if obj2.C != nil {
		t.Fatalf("%+v", obj2)
	}
	if obj2.D != nil {
		t.Fatalf("%+v", obj2)
	}
	if obj2.E != nil {
		t.Fatalf("%+v", obj2)
	}
}
