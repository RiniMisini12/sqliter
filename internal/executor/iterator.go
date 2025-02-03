package executor

import (
	"errors"

	"github.com/rinimisini112/sqliter/internal/storage"
)

type ResultIterator interface {
	Next() (*storage.Row, error)
}

type SimpleRowIterator struct {
	rows  []*storage.Row
	index int
}

func (it *SimpleRowIterator) Next() (*storage.Row, error) {
	if it.index >= len(it.rows) {
		return nil, errors.New("no more rows")
	}
	row := it.rows[it.index]
	it.index++
	return row, nil
}
