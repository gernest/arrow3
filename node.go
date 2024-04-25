package arrow3

import (
	"fmt"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"google.golang.org/protobuf/reflect/protoreflect"
)

type node struct {
	parent   *node
	build    array.Builder
	field    arrow.Field
	desc     protoreflect.Descriptor
	children []*node
}

func build(msg protoreflect.Message) *message {
	root := &node{desc: msg.Descriptor()}
	fields := msg.Descriptor().Fields()
	root.children = make([]*node, fields.Len())
	a := make([]arrow.Field, fields.Len())
	for i := 0; i < fields.Len(); i++ {
		root.children[i] = createNode(root, fields.Get(i))
		a[i] = root.children[i].field
	}
	return &message{
		root:   root,
		schema: arrow.NewSchema(a, nil),
	}
}

type message struct {
	root   *node
	schema *arrow.Schema
}

func createNode(parent *node, field protoreflect.FieldDescriptor) *node {
	fmt.Println("===== here 0", field.Name())

	n := &node{parent: parent, desc: field, field: arrow.Field{
		Name:     string(field.Name()),
		Nullable: nullable(field),
		Type:     baseType(field),
	}}

	if n.field.Type != nil {
		return n
	}
	// Try a message
	if msg := field.Message(); msg != nil {
		f := msg.Fields()
		n.children = make([]*node, f.Len())
		a := make([]arrow.Field, f.Len())
		for i := 0; i < f.Len(); i++ {
			n.children[i] = createNode(n, f.Get(i))
			a[i] = n.children[i].field
		}
		n.field.Type = arrow.StructOf(a...)
		n.field.Nullable = true
		if field.IsList() {
			n.field.Type = arrow.ListOf(n.field.Type)
		}
		return n
	}

	panic(fmt.Sprintf("%v is not supported ", field.Name()))
}

func baseType(field protoreflect.FieldDescriptor) (t arrow.DataType) {
	switch field.Kind() {
	case protoreflect.BoolKind:
		t = arrow.FixedWidthTypes.Boolean
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		t = arrow.PrimitiveTypes.Int32
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		t = arrow.PrimitiveTypes.Uint32
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		t = arrow.PrimitiveTypes.Int64
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		t = arrow.PrimitiveTypes.Uint64
	case protoreflect.DoubleKind:
		t = arrow.PrimitiveTypes.Float64
	case protoreflect.FloatKind:
		t = arrow.PrimitiveTypes.Float32
	case protoreflect.StringKind:
		t = arrow.BinaryTypes.String
	case protoreflect.BytesKind:
		t = arrow.BinaryTypes.Binary
	}
	if field.IsList() {
		if t != nil {
			t = arrow.ListOf(t)
		}
		return
	}
	if field.IsMap() {
		key := baseType(field.MapKey())
		value := baseType(field.MapValue())
		if value == nil {
			panic(fmt.Sprintf("%v is not supported as map value", field.MapValue().Kind()))
		}
		t = arrow.MapOf(key, value)
	}
	return
}

func nullable(f protoreflect.FieldDescriptor) bool {
	return f.HasOptionalKeyword() ||
		f.Kind() == protoreflect.BytesKind
}
