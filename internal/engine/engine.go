package engine

import (
	"os"

	"github.com/rinimisini112/sqliter/internal/storage"
)

type Engine struct {
	file      *os.File
	allocator *storage.Allocator
	buffer    *storage.LRUBufferManager
}

func NewEngine(path string) (*Engine, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}

	fi, err := file.Stat()
	if err != nil {
		return nil, err
	}

	minSize := int64(storage.HeaderSize + storage.MetadataPages*storage.PageSize)
	if fi.Size() < minSize {
		if err := storage.WriteFileHeader(file); err != nil {
			return nil, err
		}

		emptyMetadata := make([]byte, storage.MetadataPages*storage.PageSize)
		if _, err := file.Write(emptyMetadata); err != nil {
			return nil, err
		}
	}

	alloc, err := storage.NewAllocator(file)
	if err != nil {
		return nil, err
	}

	buffer := storage.NewLRUBufferManager(file, 100)

	engine := &Engine{
		file:      file,
		allocator: alloc,
		buffer:    buffer,
	}

	return engine, nil
}

func (e *Engine) ReadPage(pageNum int) (*storage.Page, error) {
	return e.buffer.GetPage(pageNum)
}

func (e *Engine) WritePage(pageNum int) error {
	return e.buffer.FlushPage(pageNum)
}

func (e *Engine) Flush() error {
	return e.buffer.FlushAll()
}

func (e *Engine) Close() error {
	if err := e.Flush(); err != nil {
		return err
	}
	return e.file.Close()
}
