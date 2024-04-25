package arrow3

import (
	"errors"
	"os"
	"testing"

	"github.com/gernest/arrow3/gen/go/samples"
)

func TestMessage_scalar(t *testing.T) {
	m := &samples.ScalarTypes{}
	msg := build(m.ProtoReflect())
	schema := msg.schema.String()
	match(t, "testdata/scalar.txt", schema)
}
func TestMessage_scalarOptional(t *testing.T) {
	m := &samples.ScalarTypesOptional{}
	msg := build(m.ProtoReflect())
	schema := msg.schema.String()
	match(t, "testdata/scalar_optional.txt", schema)
}
func TestMessage_scalarRepeated(t *testing.T) {
	m := &samples.ScalarTypesRepeated{}
	msg := build(m.ProtoReflect())
	schema := msg.schema.String()
	match(t, "testdata/scalar_repeated.txt", schema)
}
func TestMessage_scalarMap(t *testing.T) {
	m := &samples.ScalarTypesMap{}
	msg := build(m.ProtoReflect())
	schema := msg.schema.String()
	match(t, "testdata/scalar_map.txt", schema)
}
func TestMessage_Nested00(t *testing.T) {
	m := &samples.Nested{}
	msg := build(m.ProtoReflect())
	schema := msg.schema.String()
	match(t, "testdata/nested00.txt", schema)
}
func TestMessage_Cyclic(t *testing.T) {
	m := &samples.Cyclic{}

	err := func() (err error) {
		defer func() {
			err = recover().(error)
		}()
		build(m.ProtoReflect())
		return nil
	}()
	if !errors.Is(err, ErrMxDepth) {
		t.Errorf("expected %v got %v", ErrMxDepth, err)
	}
}

func match(t testing.TB, path string, value string, write ...struct{}) {
	t.Helper()
	if len(write) > 0 {
		os.WriteFile(path, []byte(value), 0600)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Errorf("failed reading file %s", path)
	}
	if string(b) != value {
		t.Errorf("------> want \n%s\n------> got\n%s", string(b), value)
	}
}
