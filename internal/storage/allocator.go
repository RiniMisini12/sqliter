package storage

import (
	"os"
)

type Allocator struct {
	metadata *Metadata
	file     *os.File
	numPages int
}

func NewAllocator(file *os.File) (*Allocator, error) {
	alloc := &Allocator{
		metadata: &Metadata{
			FreePages: []int{},
		},
		file:     file,
		numPages: calculateNumDataPages(file),
	}
	if err := alloc.loadMetadata(); err != nil {
	}
	return alloc, nil
}

func (a *Allocator) loadMetadata() error {
	offset := int64(HeaderSize)
	size := MetadataPages * PageSize
	data := make([]byte, size)
	if _, err := a.file.Seek(offset, 0); err != nil {
		return err
	}
	n, err := a.file.Read(data)
	if err != nil {
		return err
	}
	if n < size {
		return nil
	}
	return a.metadata.Deserialize(data)
}

func (a *Allocator) saveMetadata() error {
	data, err := a.metadata.Serialize()
	if err != nil {
		return err
	}
	offset := int64(HeaderSize)
	if _, err := a.file.Seek(offset, 0); err != nil {
		return err
	}
	_, err = a.file.Write(data)
	return err
}

func (a *Allocator) AllocatePage() (int, error) {
	if len(a.metadata.FreePages) > 0 {
		pageNum := a.metadata.FreePages[0]
		a.metadata.FreePages = a.metadata.FreePages[1:]
		if err := a.saveMetadata(); err != nil {
			return 0, err
		}
		return pageNum, nil
	}
	pageNum := a.numPages
	a.numPages++

	emptyPage := make([]byte, PageSize)
	offset := int64(HeaderSize + MetadataPages*PageSize + pageNum*PageSize)
	if _, err := a.file.Seek(offset, 0); err != nil {
		return 0, err
	}
	if _, err := a.file.Write(emptyPage); err != nil {
		return 0, err
	}
	return pageNum, nil
}

func (a *Allocator) FreePage(pageNum int) error {
	a.metadata.FreePages = append(a.metadata.FreePages, pageNum)
	return a.saveMetadata()
}

func calculateNumDataPages(file *os.File) int {
	fi, err := file.Stat()
	if err != nil {
		return 0
	}
	fileSize := fi.Size()
	minSize := int64(HeaderSize + MetadataPages*PageSize)
	if fileSize < minSize {
		return 0
	}
	return int((fileSize - minSize) / PageSize)
}
