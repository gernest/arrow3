package arrow3

import (
	"context"
	"io"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/apache/arrow/go/v17/parquet"
	"github.com/apache/arrow/go/v17/parquet/file"
	"github.com/apache/arrow/go/v17/parquet/pqarrow"
	"github.com/apache/arrow/go/v17/parquet/schema"
)

func (msg *message) Parquet() *schema.Schema {
	return msg.parquet
}

// Read reads specified columns from parquet source r. The returned record must
// be released by the caller.
func (msg *message) Read(ctx context.Context, r parquet.ReaderAtSeeker, columns []int) (arrow.Record, error) {
	f, err := file.NewParquetReader(r)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	pr, err := pqarrow.NewFileReader(f, pqarrow.ArrowReadProperties{
		BatchSize: f.NumRows(), // we read full columns
	}, memory.DefaultAllocator)
	if err != nil {
		return nil, err
	}
	rd, err := pr.GetRecordReader(ctx, columns, []int{0})
	if err != nil {
		return nil, err
	}
	defer rd.Release()
	o, err := rd.Read()
	if err != nil {
		return nil, err
	}
	o.Retain()
	return o, nil
}

// WriteParquet writes existing record as parquet file to w.
func (msg *message) WriteParquet(w io.Writer) error {
	f, err := pqarrow.NewFileWriter(msg.schema, w,
		parquet.NewWriterProperties(msg.props...),
		pqarrow.NewArrowWriterProperties(),
	)
	if err != nil {
		return err
	}
	r := msg.NewRecord()
	defer r.Release()
	err = f.Write(r)
	if err != nil {
		return err
	}
	return f.Close()
}
