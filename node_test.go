package arrow3

import (
	"os"
	"testing"

	"github.com/gernest/arrow3/gen/go/samples"
)

func TestMessage_scala(t *testing.T) {
	m := &samples.ScalarTypes{}
	msg := build(m.ProtoReflect())
	schema := msg.schema.String()
	os.WriteFile("testdata/scalar.txt", []byte(schema), 0600)
	match(t, "testdata/scalar.txt", schema)
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
