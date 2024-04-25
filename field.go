package protoarrow

import (
	"github.com/apache/arrow/go/v16/arrow"
	"google.golang.org/protobuf/reflect/protoreflect"
)

type Field2 interface {
	protoreflect.Descriptor
	Number() protoreflect.FieldNumber
	Append(protoreflect.Value)
	Type() arrow.Type
	Build() arrow.Array
}
