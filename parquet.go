package arrow3

import (
	"github.com/apache/arrow/go/v17/parquet/schema"
)

func (msg *message) Parquet() *schema.Schema {
	return msg.parquet
}
