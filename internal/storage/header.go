package storage

import (
	"encoding/binary"
	"os"
)

const (
	PageSize      = 4096
	FileMagic     = "SQLITER"
	MetadataPages = 1
	FileVersion   = uint32(1)
	HeaderSize    = 32
)

type FileHeader struct {
	Magic    [8]byte
	Version  uint32
	PageSize uint32
}

type PageType uint8

const (
	PageTypeData PageType = iota
	PageTypeIndex
	PageTypeFree
)

type PageHeader struct {
	Type        PageType
	RecordCount uint16
}

func WriteFileHeader(file *os.File) error {
	var header FileHeader

	copy(header.Magic[:], FileMagic)
	header.Version = FileVersion
	header.PageSize = PageSize

	err := binary.Write(file, binary.LittleEndian, &header)
	if err != nil {
		return err
	}
	return nil
}
