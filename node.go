package arrow3

import (
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
	field.Index()
	n := &node{parent: parent, desc: field, field: arrow.Field{
		Name:     string(field.Name()),
		Nullable: nullable(field),
		Type:     baseType(field),
	}}
	if n.field.Type != nil {
		return n
	}
	panic("Only base types are supported")
}

func baseType(field protoreflect.FieldDescriptor) arrow.DataType {
	switch field.Kind() {
	case protoreflect.BoolKind:
		return arrow.FixedWidthTypes.Boolean
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		return arrow.PrimitiveTypes.Int32
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		return arrow.PrimitiveTypes.Uint32
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		return arrow.PrimitiveTypes.Int64
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		return arrow.PrimitiveTypes.Uint64
	case protoreflect.DoubleKind:
		return arrow.PrimitiveTypes.Float64
	case protoreflect.FloatKind:
		return arrow.PrimitiveTypes.Float32
	case protoreflect.StringKind:
		return arrow.BinaryTypes.String
	case protoreflect.BytesKind:
		return arrow.BinaryTypes.Binary
	default:
		return nil
	}
}

func nullable(f protoreflect.FieldDescriptor) bool {
	if f.HasOptionalKeyword() || f.Cardinality() == protoreflect.Repeated || f.IsMap() {
		return true
	}
	switch f.Kind() {
	case protoreflect.BytesKind, protoreflect.MessageKind, protoreflect.GroupKind:
		return true
	default:
		return false
	}
}
