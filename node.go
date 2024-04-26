package arrow3

import (
	"errors"
	"fmt"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	maxDepth = 5
)

var ErrMxDepth = errors.New("max depth reached, either the message is deeply nested or a circular dependency was introduced")

type valueFn func(protoreflect.Value) error

type node struct {
	parent   *node
	field    arrow.Field
	setup    func(array.Builder) valueFn
	write    valueFn
	desc     protoreflect.Descriptor
	children []*node
}

func build(msg protoreflect.Message) *message {
	root := &node{desc: msg.Descriptor(),
		field: arrow.Field{},
	}
	fields := msg.Descriptor().Fields()
	root.children = make([]*node, fields.Len())
	a := make([]arrow.Field, fields.Len())
	for i := 0; i < fields.Len(); i++ {
		root.children[i] = createNode(root, fields.Get(i), 0)
		a[i] = root.children[i].field
	}
	return &message{
		root:   root,
		schema: arrow.NewSchema(a, nil),
	}
}

type message struct {
	root    *node
	schema  *arrow.Schema
	builder *array.RecordBuilder
}

func (m *message) build(mem memory.Allocator) {
	b := array.NewRecordBuilder(mem, m.schema)
	for i, ch := range m.root.children {
		ch.build(b.Field(i))
	}
	m.builder = b
}

func (m *message) append(msg protoreflect.Message) {
	m.root.WriteMessage(msg)
}

func (m *message) NewRecord() arrow.Record {
	return m.builder.NewRecord()
}
func createNode(parent *node, field protoreflect.FieldDescriptor, depth int) *node {
	if depth >= maxDepth {
		panic(ErrMxDepth)
	}
	name, ok := parent.field.Metadata.GetValue("path")
	if ok {
		name += "." + string(field.Name())
	} else {
		name = string(field.Name())
	}
	fmt.Println(name)
	n := &node{parent: parent, desc: field, field: arrow.Field{
		Name:     string(field.Name()),
		Nullable: nullable(field),
		Metadata: arrow.MetadataFrom(map[string]string{
			"path": name,
		}),
	}}
	n.field.Type = n.baseType(field)

	if n.field.Type != nil {
		return n
	}
	// Try a message
	if msg := field.Message(); msg != nil {
		if isDuration(msg) {
			n.field.Type = arrow.FixedWidthTypes.Duration_ms
			n.field.Nullable = true
			n.setup = func(b array.Builder) valueFn {
				a := b.(*array.DurationBuilder)
				return func(v protoreflect.Value) error {
					if !v.IsValid() {
						a.AppendNull()
						return nil
					}
					e := v.Message().Interface().(*durationpb.Duration)
					a.Append(arrow.Duration(e.AsDuration().Milliseconds()))
					return nil
				}
			}
		}
		if isTs(msg) {
			n.field.Type = arrow.FixedWidthTypes.Timestamp_ms
			n.field.Nullable = true
			n.setup = func(b array.Builder) valueFn {
				a := b.(*array.TimestampBuilder)
				return func(v protoreflect.Value) error {
					if !v.IsValid() {
						a.AppendNull()
						return nil
					}
					e := v.Message().Interface().(*timestamppb.Timestamp)
					a.Append(arrow.Timestamp(e.AsTime().UTC().UnixMilli()))
					return nil
				}
			}
		}
		if n.field.Type != nil {
			if field.IsList() {
				n.field.Type = arrow.ListOf(n.field.Type)
				setup := n.setup
				n.setup = func(b array.Builder) valueFn {
					ls := b.(*array.ListBuilder)
					value := setup(ls.ValueBuilder())
					return func(v protoreflect.Value) error {
						if !v.IsValid() {
							ls.AppendNull()
							return nil
						}
						ls.Append(true)
						list := v.List()
						for i := 0; i < list.Len(); i++ {
							err := value(list.Get(i))
							if err != nil {
								return err
							}
						}
						return nil
					}
				}
			}
			return n
		}
		f := msg.Fields()
		n.children = make([]*node, f.Len())
		a := make([]arrow.Field, f.Len())
		for i := 0; i < f.Len(); i++ {
			n.children[i] = createNode(n, f.Get(i), depth+1)
			a[i] = n.children[i].field
		}
		n.field.Type = arrow.StructOf(a...)
		n.field.Nullable = true
		n.setup = func(b array.Builder) valueFn {
			a := b.(*array.StructBuilder)
			fs := make([]valueFn, len(n.children))
			for i := range n.children {
				fs[i] = n.children[i].setup(a.FieldBuilder(i))
			}
			return func(v protoreflect.Value) error {
				if !v.IsValid() {
					a.AppendNull()
					return nil
				}
				a.Append(true)
				msg := v.Message()
				fields := msg.Descriptor().Fields()
				for i := 0; i < fields.Len(); i++ {
					err := fs[i](msg.Get(fields.Get(i)))
					if err != nil {
						return err
					}
				}
				return nil
			}
		}
		if field.IsList() {
			n.field.Type = arrow.ListOf(n.field.Type)
			setup := n.setup
			n.setup = func(b array.Builder) valueFn {
				ls := b.(*array.ListBuilder)
				value := setup(ls.ValueBuilder())
				return func(v protoreflect.Value) error {
					if !v.IsValid() {
						ls.AppendNull()
						return nil
					}
					ls.Append(true)
					list := v.List()
					for i := 0; i < list.Len(); i++ {
						err := value(list.Get(i))
						if err != nil {
							return err
						}
					}
					return nil
				}

			}
		}
		return n
	}
	panic(fmt.Sprintf("%v is not supported ", field.Name()))
}

func (n *node) build(a array.Builder) {
	n.write = n.setup(a)
}

func (n *node) WriteMessage(msg protoreflect.Message) {
	f := msg.Descriptor().Fields()
	for i := 0; i < f.Len(); i++ {
		n.children[i].write(msg.Get(f.Get(i)))
	}
}

func (n *node) baseType(field protoreflect.FieldDescriptor) (t arrow.DataType) {
	switch field.Kind() {
	case protoreflect.BoolKind:
		t = arrow.FixedWidthTypes.Boolean

		n.setup = func(b array.Builder) valueFn {
			a := b.(*array.BooleanBuilder)
			return func(v protoreflect.Value) error {
				a.Append(v.Bool())
				return nil
			}
		}
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		t = arrow.PrimitiveTypes.Int32
		n.setup = func(b array.Builder) valueFn {
			a := b.(*array.Int32Builder)
			return func(v protoreflect.Value) error {
				a.Append(int32(v.Int()))
				return nil
			}
		}
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		n.setup = func(b array.Builder) valueFn {
			a := b.(*array.Uint32Builder)
			return func(v protoreflect.Value) error {
				a.Append(uint32(v.Uint()))
				return nil
			}
		}
		t = arrow.PrimitiveTypes.Uint32
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		n.setup = func(b array.Builder) valueFn {
			a := b.(*array.Int64Builder)
			return func(v protoreflect.Value) error {
				a.Append(v.Int())
				return nil
			}
		}
		t = arrow.PrimitiveTypes.Int64
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		n.setup = func(b array.Builder) valueFn {
			a := b.(*array.Uint64Builder)
			return func(v protoreflect.Value) error {
				a.Append(v.Uint())
				return nil
			}
		}
		t = arrow.PrimitiveTypes.Uint64
	case protoreflect.DoubleKind:
		n.setup = func(b array.Builder) valueFn {
			a := b.(*array.Float64Builder)
			return func(v protoreflect.Value) error {
				a.Append(v.Float())
				return nil
			}
		}
		t = arrow.PrimitiveTypes.Float64
	case protoreflect.FloatKind:
		n.setup = func(b array.Builder) valueFn {
			a := b.(*array.Float32Builder)
			return func(v protoreflect.Value) error {
				a.Append(float32(v.Float()))
				return nil
			}
		}
		t = arrow.PrimitiveTypes.Float32
	case protoreflect.StringKind:
		n.setup = func(b array.Builder) valueFn {
			a := b.(*array.StringBuilder)
			return func(v protoreflect.Value) error {
				a.Append(v.String())
				return nil
			}
		}
		t = arrow.BinaryTypes.String
	case protoreflect.BytesKind:
		n.setup = func(b array.Builder) valueFn {
			a := b.(*array.BinaryBuilder)
			return func(v protoreflect.Value) error {
				if !v.IsValid() {
					a.AppendNull()
					return nil
				}
				a.Append(v.Bytes())
				return nil
			}
		}
		t = arrow.BinaryTypes.Binary
	}
	if field.IsList() {
		if t != nil {
			setup := n.setup
			n.setup = func(b array.Builder) valueFn {
				ls := b.(*array.ListBuilder)
				vb := setup(ls.ValueBuilder())
				return func(v protoreflect.Value) error {
					if !v.IsValid() {
						ls.AppendNull()
						return nil
					}
					ls.Append(true)
					list := v.List()
					for i := 0; i < list.Len(); i++ {
						err := vb(list.Get(i))
						if err != nil {
							return err
						}
					}
					return nil
				}
			}
			t = arrow.ListOf(t)
		}
		return
	}
	if field.IsMap() {
		key := n.baseType(field.MapKey())
		keySet := n.setup
		value := n.baseType(field.MapValue())
		if value == nil {
			panic(fmt.Sprintf("%v is not supported as map value", field.MapValue().Kind()))
		}
		valueSet := n.setup
		n.setup = func(b array.Builder) valueFn {
			a := b.(*array.MapBuilder)
			key := keySet(a.KeyBuilder())
			value := valueSet(a.ItemBuilder())
			return func(v protoreflect.Value) error {
				if !v.IsValid() {
					a.AppendNull()
					return nil
				}
				a.Append(true)
				m := v.Map()
				m.Range(func(mk protoreflect.MapKey, v protoreflect.Value) bool {
					key(protoreflect.Value(mk))
					value(v)
					return true
				})
				return nil
			}
		}
		t = arrow.MapOf(key, value)
	}
	return
}

var (
	ts       = &timestamppb.Timestamp{}
	duration = &durationpb.Duration{}
)

func isTs(msg protoreflect.MessageDescriptor) bool {
	return ts.ProtoReflect().Descriptor() == msg
}

func isDuration(msg protoreflect.MessageDescriptor) bool {
	return duration.ProtoReflect().Descriptor() == msg
}

func nullable(f protoreflect.FieldDescriptor) bool {
	return f.HasOptionalKeyword() ||
		f.Kind() == protoreflect.BytesKind
}
