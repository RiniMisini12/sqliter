package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
)

type Metadata struct {
	FreePages []int
}

func (m *Metadata) Serialize() ([]byte, error) {
	buf := new(bytes.Buffer)

	count := int32(len(m.FreePages))
	if err := binary.Write(buf, binary.LittleEndian, count); err != nil {
		return nil, err
	}

	for _, pageNum := range m.FreePages {
		if err := binary.Write(buf, binary.LittleEndian, int32(pageNum)); err != nil {
			return nil, err
		}
	}

	data := buf.Bytes()
	requiredSize := MetadataPages * PageSize
	if len(data) > requiredSize {
		return nil, errors.New("metadata size exceeds allocated space")
	}

	padded := make([]byte, requiredSize)
	copy(padded, data)

	return padded, nil
}

func (m *Metadata) Deserialize(data []byte) error {
	buf := bytes.NewReader(data)

	var count int32
	if err := binary.Read(buf, binary.LittleEndian, &count); err != nil {
		return err
	}

	m.FreePages = make([]int, count)
	for i := 0; i < int(count); i++ {
		var pageNum int32
		if err := binary.Read(buf, binary.LittleEndian, &pageNum); err != nil {
			return err
		}
		m.FreePages[i] = int(pageNum)
	}
	return nil
}
