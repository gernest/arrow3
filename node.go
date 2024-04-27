package arrow3

import (
	"errors"
	"fmt"
	"math"
	"strconv"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/apache/arrow/go/v17/parquet"
	"github.com/apache/arrow/go/v17/parquet/compress"
	"github.com/apache/arrow/go/v17/parquet/pqarrow"
	"github.com/apache/arrow/go/v17/parquet/schema"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

const (
	maxDepth = 10
)

var ErrMxDepth = errors.New("max depth reached, either the message is deeply nested or a circular dependency was introduced")

type valueFn func(protoreflect.Value) error

type encodeFn func(msg protoreflect.Message, a arrow.Array, i int) protoreflect.Value

type node struct {
	parent   *node
	field    arrow.Field
	setup    func(array.Builder) valueFn
	write    valueFn
	desc     protoreflect.Descriptor
	children []*node
	encode   encodeFn
	hash     map[string]*node
}

func unmarshal[T proto.Message](n *node, r arrow.Record, rows []int) []T {
	if rows == nil {
		rows = make([]int, r.NumRows())
		for i := range rows {
			rows[i] = i
		}
	}
	o := make([]T, len(rows))
	var a T
	ref := a.ProtoReflect()
	for _, row := range rows {
		msg := ref.New()
		for i := 0; i < int(r.NumCols()); i++ {
			name := r.ColumnName(i)
			nx, ok := n.hash[name]
			if !ok {
				panic(fmt.Sprintf("arrow3: field %s not found in node %v", name, n.field.Name))
			}
			fs := nx.desc.(protoreflect.FieldDescriptor)
			switch {
			case fs.IsList():
				nx.encode(msg, r.Column(i), row)
			case fs.IsMap():
				panic("MAP not supported")
			default:
				if fs.Message() != nil {
					nx.encode(
						msg.Mutable(fs).Message(),
						r.Column(i),
						row,
					)
				} else {
					msg.Set(fs, nx.encode(nil, r.Column(i), row))
				}
			}
		}
	}
	return o

}

func build(msg protoreflect.Message) *message {
	root := &node{desc: msg.Descriptor(),
		field: arrow.Field{},
		hash:  make(map[string]*node),
	}
	fields := msg.Descriptor().Fields()
	root.children = make([]*node, fields.Len())
	a := make([]arrow.Field, fields.Len())
	for i := 0; i < fields.Len(); i++ {
		x := createNode(root, fields.Get(i), 0)
		root.children[i] = x
		root.hash[x.field.Name] = x
		a[i] = root.children[i].field
	}
	as := arrow.NewSchema(a, nil)

	// we need to apply compression on all fields and use dictionary for binary and
	// string columns.
	bs, err := pqarrow.ToParquet(as, parquet.NewWriterProperties(), pqarrow.DefaultWriterProps())
	if err != nil {
		panic(err)
	}
	var props []parquet.WriterProperty

	for i := 0; i < bs.NumColumns(); i++ {
		col := bs.Column(i)
		if col.PhysicalType() == parquet.Types.ByteArray {
			props = append(props, parquet.WithDictionaryPath(col.ColumnPath(), true))
		}
	}
	// ZSTD is pretty good for all cases. Default level is reasonable.
	props = append(props, parquet.WithCompression(compress.Codecs.Zstd))
	// All writes are on a single row group. This is needed because we treat rows
	// are sample ID and we need to keep the mapping
	props = append(props, parquet.WithMaxRowGroupLength(math.MaxInt))

	ps, err := pqarrow.ToParquet(as, parquet.NewWriterProperties(props...), pqarrow.DefaultWriterProps())
	if err != nil {
		panic(err)
	}
	return &message{
		root:    root,
		schema:  as,
		parquet: ps,
		props:   props,
	}
}

type message struct {
	root    *node
	schema  *arrow.Schema
	parquet *schema.Schema
	builder *array.RecordBuilder
	props   []parquet.WriterProperty
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
	n := &node{parent: parent, desc: field, field: arrow.Field{
		Name:     string(field.Name()),
		Nullable: nullable(field),
		Metadata: arrow.MetadataFrom(map[string]string{
			"path":             name,
			"PARQUET:field_id": strconv.Itoa(int(field.Number())),
		}),
	}, hash: make(map[string]*node)}
	n.field.Type = n.baseType(field)

	if n.field.Type != nil {
		return n
	}
	// Try a message
	if msg := field.Message(); msg != nil {
		switch msg {
		case otelAnyDescriptor:
			n.field.Type = arrow.BinaryTypes.Binary
			n.field.Nullable = true
			n.setup = func(b array.Builder) valueFn {
				a := b.(*array.BinaryBuilder)
				return func(v protoreflect.Value) error {
					if !v.IsValid() {
						a.AppendNull()
						return nil
					}
					e := v.Message().Interface().(*commonv1.AnyValue)
					bs, err := proto.Marshal(e)
					if err != nil {
						return err
					}
					a.Append(bs)
					return nil
				}
			}
			n.encode = func(msg protoreflect.Message, a arrow.Array, i int) protoreflect.Value {
				if a.IsNull(i) {
					return protoreflect.Value{}
				}
				var v []byte
				if a.DataType().ID() == arrow.DICTIONARY {
					d := a.(*array.Dictionary)
					v = d.Dictionary().(*array.Binary).Value(d.GetValueIndex(i))
				} else {
					v = a.(*array.Binary).Value(i)
				}
				proto.Unmarshal(v, msg.Interface())
				return protoreflect.Value{}
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
				encode := n.encode
				n.encode = func(msg protoreflect.Message, a arrow.Array, i int) protoreflect.Value {
					ls := a.(*array.List)
					start, end := ls.ValueOffsets(i)
					if start == end {
						return protoreflect.Value{}
					}
					v := ls.ListValues()
					list := msg.Mutable(field).List()
					for j := start; j < end; j++ {
						encode(list.NewElement().Message(), v, int(j))
					}
					return protoreflect.Value{}
				}
			}
			return n
		}
		f := msg.Fields()
		n.children = make([]*node, f.Len())
		a := make([]arrow.Field, f.Len())
		for i := 0; i < f.Len(); i++ {
			x := createNode(n, f.Get(i), depth+1)
			n.children[i] = x
			n.hash[x.field.Name] = x
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
		n.encode = func(msg protoreflect.Message, a arrow.Array, i int) protoreflect.Value {
			s := a.(*array.Struct)
			typ := a.DataType().(*arrow.StructType)
			for j := 0; j < s.NumField(); j++ {
				f := typ.Field(j)
				nx, ok := n.hash[f.Name]
				if !ok {
					panic(fmt.Sprintf("arrow3: field %s not found in node %v", f.Name, n.field.Name))
				}
				fs := nx.desc.(protoreflect.FieldDescriptor)
				switch {
				case fs.IsList():
					nx.encode(msg, s.Field(j), i)
				case fs.IsMap():
					panic("MAP not supported")
				default:
					if fs.Message() != nil {
						nx.encode(
							msg.Mutable(fs).Message(),
							s.Field(j),
							i,
						)
					} else {
						msg.Set(fs, nx.encode(nil, s.Field(j), i))
					}
				}

			}
			return protoreflect.Value{}
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
			encode := n.encode
			n.encode = func(msg protoreflect.Message, a arrow.Array, i int) protoreflect.Value {
				ls := a.(*array.List)
				start, end := ls.ValueOffsets(i)
				if start == end {
					return protoreflect.Value{}
				}
				v := ls.ListValues()
				list := msg.Mutable(field).List()
				for j := start; j < end; j++ {
					encode(list.NewElement().Message(),
						v, i,
					)
				}
				return protoreflect.Value{}
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
	case protoreflect.EnumKind:
		t = arrow.PrimitiveTypes.Int32

		n.setup = func(b array.Builder) valueFn {
			a := b.(*array.Int32Builder)
			return func(v protoreflect.Value) error {
				a.Append(int32(v.Enum()))
				return nil
			}
		}
		n.encode = func(msg protoreflect.Message, a arrow.Array, i int) protoreflect.Value {
			return protoreflect.ValueOfEnum(
				protoreflect.EnumNumber(
					a.(*array.Int32).Value(i),
				),
			)
		}
	case protoreflect.BoolKind:
		t = arrow.FixedWidthTypes.Boolean

		n.setup = func(b array.Builder) valueFn {
			a := b.(*array.BooleanBuilder)
			return func(v protoreflect.Value) error {
				a.Append(v.Bool())
				return nil
			}
		}
		n.encode = func(msg protoreflect.Message, a arrow.Array, i int) protoreflect.Value {
			return protoreflect.ValueOfBool(a.(*array.Boolean).Value(i))
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
		n.encode = func(msg protoreflect.Message, a arrow.Array, i int) protoreflect.Value {
			return protoreflect.ValueOfInt32(a.(*array.Int32).Value(i))
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
		n.encode = func(msg protoreflect.Message, a arrow.Array, i int) protoreflect.Value {
			return protoreflect.ValueOfUint32(a.(*array.Uint32).Value(i))
		}
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		n.setup = func(b array.Builder) valueFn {
			a := b.(*array.Int64Builder)
			return func(v protoreflect.Value) error {
				a.Append(v.Int())
				return nil
			}
		}
		t = arrow.PrimitiveTypes.Int64
		n.encode = func(msg protoreflect.Message, a arrow.Array, i int) protoreflect.Value {
			return protoreflect.ValueOfInt64(a.(*array.Int64).Value(i))
		}
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		n.setup = func(b array.Builder) valueFn {
			a := b.(*array.Uint64Builder)
			return func(v protoreflect.Value) error {
				a.Append(v.Uint())
				return nil
			}
		}
		t = arrow.PrimitiveTypes.Uint64
		n.encode = func(msg protoreflect.Message, a arrow.Array, i int) protoreflect.Value {
			return protoreflect.ValueOfUint64(a.(*array.Uint64).Value(i))
		}
	case protoreflect.DoubleKind:
		n.setup = func(b array.Builder) valueFn {
			a := b.(*array.Float64Builder)
			return func(v protoreflect.Value) error {
				a.Append(v.Float())
				return nil
			}
		}
		t = arrow.PrimitiveTypes.Float64
		n.encode = func(msg protoreflect.Message, a arrow.Array, i int) protoreflect.Value {
			return protoreflect.ValueOfFloat64(a.(*array.Float64).Value(i))
		}
	case protoreflect.FloatKind:
		n.setup = func(b array.Builder) valueFn {
			a := b.(*array.Float32Builder)
			return func(v protoreflect.Value) error {
				a.Append(float32(v.Float()))
				return nil
			}
		}
		t = arrow.PrimitiveTypes.Float32
		n.encode = func(msg protoreflect.Message, a arrow.Array, i int) protoreflect.Value {
			return protoreflect.ValueOfFloat32(a.(*array.Float32).Value(i))
		}
	case protoreflect.StringKind:
		n.setup = func(b array.Builder) valueFn {
			a := b.(*array.StringBuilder)
			return func(v protoreflect.Value) error {
				a.Append(v.String())
				return nil
			}
		}
		t = arrow.BinaryTypes.String
		n.encode = func(msg protoreflect.Message, a arrow.Array, i int) protoreflect.Value {
			if a.DataType().ID() == arrow.DICTIONARY {
				d := a.(*array.Dictionary)
				return protoreflect.ValueOfString(
					d.Dictionary().(*array.String).Value(d.GetValueIndex(i)),
				)
			}
			return protoreflect.ValueOfBool(a.(*array.Boolean).Value(i))
		}
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
		n.encode = func(msg protoreflect.Message, a arrow.Array, i int) protoreflect.Value {
			if a.DataType().ID() == arrow.DICTIONARY {
				d := a.(*array.Dictionary)
				return protoreflect.ValueOfBytes(
					d.Dictionary().(*array.Binary).Value(d.GetValueIndex(i)),
				)
			}
			return protoreflect.ValueOfBytes(a.(*array.Binary).Value(i))
		}
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
			encode := n.encode
			n.encode = func(msg protoreflect.Message, a arrow.Array, i int) protoreflect.Value {
				ls := a.(*array.List)
				start, end := ls.ValueOffsets(i)
				if start == end {
					return protoreflect.Value{}
				}
				v := ls.ListValues()
				list := msg.Mutable(field).List()
				for j := start; i < int(end); i++ {
					list.Append(
						// we are intentionally passing nil message to make sure no base types
						// mess with msg.
						encode(nil, v, int(j)),
					)
				}
				return protoreflect.Value{}
			}
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
		n.encode = func(msg protoreflect.Message, a arrow.Array, i int) protoreflect.Value {
			panic("MAP is not supported for decoding")
		}
	}
	return
}

func nullable(f protoreflect.FieldDescriptor) bool {
	return f.HasOptionalKeyword() ||
		f.Kind() == protoreflect.BytesKind
}
