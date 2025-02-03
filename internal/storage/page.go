package storage

import (
	"encoding/binary"
	"errors"
	"os"
)

type Page struct {
	Header PageHeader
	Data   []byte
}

func PageHeaderSize() int {
	return binary.Size(PageHeader{})
}

func WritePage(file *os.File, pageNumber int, page *Page) error {
	offset := int64(HeaderSize) + int64(pageNumber*PageSize)
	_, err := file.Seek(offset, 0)
	if err != nil {
		return err
	}

	if err := binary.Write(file, binary.LittleEndian, &page.Header); err != nil {
		return err
	}

	headerSize := PageHeaderSize()
	dataSize := PageSize - headerSize

	if len(page.Data) > dataSize {
		return errors.New("page data size exceeds maximum page size")
	}

	paddedData := make([]byte, dataSize)
	copy(paddedData, page.Data)

	_, err = file.Write(paddedData)
	return err
}

func ReadPage(file *os.File, pageNumber int) (*Page, error) {
	offset := int64(HeaderSize) + int64(pageNumber*PageSize)
	_, err := file.Seek(offset, 0)
	if err != nil {
		return nil, err
	}

	var header PageHeader
	if err := binary.Read(file, binary.LittleEndian, &header); err != nil {
		return nil, err
	}

	HeaderSize := PageHeaderSize()
	dataSize := PageSize - HeaderSize
	data := make([]byte, dataSize)

	if _, err := file.Read(data); err != nil {
		return nil, err
	}

	return &Page{Header: header, Data: data}, nil
}
