package arrow3

import (
	"github.com/apache/arrow/go/v17/parquet"
	"github.com/apache/arrow/go/v17/parquet/pqarrow"
	"github.com/apache/arrow/go/v17/parquet/schema"
)

func (msg *message) toParquet() (*schema.Schema, error) {
	return pqarrow.ToParquet(msg.schema, parquet.NewWriterProperties(), pqarrow.DefaultWriterProps())
}
