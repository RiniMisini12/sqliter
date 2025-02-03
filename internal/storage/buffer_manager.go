package storage

import (
	"container/list"
	"errors"
	"os"
)

type cacheEntry struct {
	pageNum  int
	page     *Page
	pinCount int
}

type LRUBufferManager struct {
	file     *os.File
	capacity int
	cache    map[int]*list.Element
	lruList  *list.List
}

func NewLRUBufferManager(file *os.File, capacity int) *LRUBufferManager {
	return &LRUBufferManager{
		file:     file,
		capacity: capacity,
		cache:    make(map[int]*list.Element),
		lruList:  list.New(),
	}
}

func (bm *LRUBufferManager) GetPage(pageNum int) (*Page, error) {
	if elem, ok := bm.cache[pageNum]; ok {
		bm.lruList.MoveToFront(elem)
		entry := elem.Value.(*cacheEntry)
		entry.pinCount++
		return entry.page, nil
	}

	page, err := ReadPage(bm.file, pageNum)
	if err != nil {
		return nil, err
	}

	if bm.lruList.Len() >= bm.capacity {
		if err := bm.evict(); err != nil {
			return nil, err
		}
	}

	entry := &cacheEntry{
		pageNum:  pageNum,
		page:     page,
		pinCount: 1,
	}
	elem := bm.lruList.PushFront(entry)
	bm.cache[pageNum] = elem

	return page, nil
}

func (bm *LRUBufferManager) UnpinPage(pageNum int) error {
	elem, ok := bm.cache[pageNum]
	if !ok {
		return errors.New("page not found in cache")
	}
	entry := elem.Value.(*cacheEntry)
	if entry.pinCount > 0 {
		entry.pinCount--
	}
	return nil
}

func (bm *LRUBufferManager) evict() error {
	for elem := bm.lruList.Back(); elem != nil; elem = elem.Prev() {
		entry := elem.Value.(*cacheEntry)
		if entry.pinCount == 0 {
			bm.lruList.Remove(elem)
			delete(bm.cache, entry.pageNum)
			return nil
		}
	}

	return errors.New("no evictable page available")
}

func (bm *LRUBufferManager) FlushPage(pageNum int) error {
	elem, ok := bm.cache[pageNum]
	if !ok {
		return errors.New("page not in cache")
	}
	entry := elem.Value.(*cacheEntry)
	return WritePage(bm.file, pageNum, entry.page)
}

func (bm *LRUBufferManager) FlushAll() error {
	for pageNum := range bm.cache {
		if err := bm.FlushPage(pageNum); err != nil {
			return err
		}
	}
	return nil
}
