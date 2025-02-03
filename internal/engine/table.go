package engine

import (
	"bytes"
	"errors"

	"github.com/rinimisini112/sqliter/internal/storage"
)

type Table struct {
	engine   *Engine
	dataPage int
}

func NewTable(engine *Engine) (*Table, error) {
	pageNum, err := engine.allocator.AllocatePage()
	if err != nil {
		return nil, err
	}

	page, err := engine.buffer.GetPage(pageNum)
	if err != nil {
		return nil, err
	}
	page.Header.RecordCount = 0
	page.Data = make([]byte, storage.PageSize-storage.PageHeaderSize())
	if err := engine.WritePage(pageNum); err != nil {
		return nil, err
	}

	return &Table{
		engine:   engine,
		dataPage: pageNum,
	}, nil
}

func (t *Table) InsertRow(row *storage.Row) error {
	serialized, err := storage.SerializeRow(row)
	if err != nil {
		return err
	}

	page, err := t.engine.buffer.GetPage(t.dataPage)
	if err != nil {
		return err
	}

	currentData := page.Data

	var buf bytes.Buffer
	buf.Write(currentData)
	buf.Write(serialized)
	newData := buf.Bytes()

	if len(newData) > (storage.PageSize - storage.PageHeaderSize()) {
		return errors.New("not enough space in page for new row")
	}

	page.Data = newData
	page.Header.RecordCount++
	return t.engine.WritePage(t.dataPage)
}

func (t *Table) GetRows() ([]*storage.Row, error) {
	page, err := t.engine.buffer.GetPage(t.dataPage)
	if err != nil {
		return nil, err
	}

	var rows []*storage.Row
	data := page.Data

	for len(data) > 0 {
		row, bytesRead, err := storage.DeserializeRowWithLength(data)
		if err != nil {
			break
		}
		rows = append(rows, row)
		data = data[bytesRead:]
	}
	return rows, nil
}
