package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
)

type Row struct {
	Values []interface{}
}

func SerializeRow(row *Row) ([]byte, error) {
	buf := new(bytes.Buffer)

	if err := binary.Write(buf, binary.LittleEndian, uint16(len(row.Values))); err != nil {
		return nil, err
	}

	for _, v := range row.Values {
		switch value := v.(type) {
		case int:
			if err := buf.WriteByte(1); err != nil {
				return nil, err
			}
			if err := binary.Write(buf, binary.LittleEndian, int64(value)); err != nil {
				return nil, err
			}
		case string:
			if err := buf.WriteByte(2); err != nil {
				return nil, err
			}
			strBytes := []byte(value)
			if err := binary.Write(buf, binary.LittleEndian, uint16(len(strBytes))); err != nil {
				return nil, err
			}
			if _, err := buf.Write(strBytes); err != nil {
				return nil, err
			}
		default:
			return nil, errors.New("unsupported type in row")
		}
	}

	recordBytes := buf.Bytes()
	finalBuf := new(bytes.Buffer)
	if err := binary.Write(finalBuf, binary.LittleEndian, uint32(len(recordBytes))); err != nil {
		return nil, err
	}
	finalBuf.Write(recordBytes)
	return finalBuf.Bytes(), nil
}

func DeserializeRow(data []byte) (*Row, error) {
	buf := bytes.NewReader(data)
	var numValues uint16
	if err := binary.Read(buf, binary.LittleEndian, &numValues); err != nil {
		return nil, err
	}
	row := &Row{Values: make([]interface{}, numValues)}
	for i := 0; i < int(numValues); i++ {
		typeMarker, err := buf.ReadByte()
		if err != nil {
			return nil, err
		}
		switch typeMarker {
		case 1:
			var intVal int64
			if err := binary.Read(buf, binary.LittleEndian, &intVal); err != nil {
				return nil, err
			}
			row.Values[i] = int(intVal)
		case 2:
			var strLen uint16
			if err := binary.Read(buf, binary.LittleEndian, &strLen); err != nil {
				return nil, err
			}
			strBytes := make([]byte, strLen)
			if _, err := buf.Read(strBytes); err != nil {
				return nil, err
			}
			row.Values[i] = string(strBytes)
		default:
			return nil, errors.New("unknown type marker during deserialization")
		}
	}
	return row, nil
}

func DeserializeRowWithLength(data []byte) (*Row, int, error) {
	if len(data) < 4 {
		return nil, 0, errors.New("not enough data to read length")
	}
	length := binary.LittleEndian.Uint32(data[:4])
	if len(data) < int(4+length) {
		return nil, 0, errors.New("data is shorter than expected record length")
	}
	row, err := DeserializeRow(data[4 : 4+length])
	if err != nil {
		return nil, 0, err
	}
	return row, int(4 + length), nil
}
