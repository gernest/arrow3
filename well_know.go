package arrow3

import (
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	tsDesc            = (&timestamppb.Timestamp{}).ProtoReflect().Descriptor()
	durationDesc      = (&durationpb.Duration{}).ProtoReflect().Descriptor()
	otelAnyDescriptor = (&commonv1.AnyValue{}).ProtoReflect().Descriptor()
)
