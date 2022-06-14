package sstable

import (
	"encoding/binary"
	"mydb/base"
	"mydb/cache" // todo: 包结构可能要改动一下
)

type Iter struct {
	sst   			*SST // 用到f, FileNum, DataPageCount
	dataPageCount 	int
	indexPageCount  int
	sstIndex        []uint64
	// indexPos就是current-data-page
	pageIndex       int // todo: 查index的结果应该返回?
	dataPageIter    *DataPageIter

	valid			bool
	cache           *cache.FIFOCache
}

// *Iter implements the TableIter interface.
var _ base.TableIter = (*Iter)(nil)

func NewSSTableIter(sst *SST, c *cache.FIFOCache) *Iter { // todo: if sst.F == nil
	dataPageCount := sst.Meta.DataPageCount
	indexPageCount := computIndexPageCount(dataPageCount)
	info, err := sst.F.Stat()
	if err != nil {
		panic(err.Error())
	}
	if int(info.Size()) != PageSize*(dataPageCount+indexPageCount+1) {
		panic("file should always be n*PageSize large!")
	}
	// todo: maybe load sstIndex later
	// todo: sstIndex cache?
	indexPageBuf := make([]byte, indexPageCount*PageSize, indexPageCount*PageSize)
	sst.F.ReadAt(indexPageBuf, int64(dataPageCount*PageSize))
	index := convertToMemorySearchStructure(indexPageBuf)

	return &Iter{
		sst:            sst,
		dataPageCount:  dataPageCount,
		indexPageCount: indexPageCount,
		sstIndex:       index,
		pageIndex:      -1,
		dataPageIter:   nil,
		valid:          false,
		cache:          c, 
	}
}

func (it *Iter)assertValid() {
	if !it.valid {
		panic("Iter is not valid!")
	}
}

func computIndexPageCount(dataPageCount int) int {
	temp := PageSize / UserKeySize // 一个indexPage对应的dataPage的数量
	ret := dataPageCount / temp
	if dataPageCount % temp != 0 {
		ret ++
	}
	return ret
}

func convertToMemorySearchStructure(buf []byte) []uint64 {
	bufLen := len(buf)
	ret := make([]uint64, bufLen/8, bufLen/8)
	for i:=0; i<len(ret); i++ {
		ret[i] = binary.LittleEndian.Uint64(buf[i*8:(i+1)*8])
	}
	return ret
}

// search init.sstIndex, set it.pageIndex
// ukey <= it.sstIndex[it.pageIndex]
func (it *Iter)searchInIndexPage(ukey uint64) (index int, found bool) {
	if it.dataPageCount == 1 { // todo:
		return 0, true
	}

	index = 0
	for j:=0; j<len(it.sstIndex); j++ {
		if ukey <= it.sstIndex[j] {
			index = j
			found = true
			break
		}
	}

	return index, found
}

func (it *Iter)First() bool {
	if it.dataPageCount == 0 {
		return false // 真的会有这种情况吗
	}
	it.loadDataPage(0)
	it.valid = true
	return true
}

func (it *Iter)SeekLe(target base.InternalKey) (find bool) {
	// search sstIndex
	// load data page
	// position data page iter
	i, found := it.searchInIndexPage(target.UserKey)
	if !found {
		// 有两种情况
		// 1. 这不是第一个index-page, (但是当前的实现是一次性加载整个index)
		// 2. 所有record都大于target
		return false
	}
	it.loadDataPage(i)
	// 如果data-page里第一个比ukey大, 则应定位到上一个data-page的最后一个
	// SeekGe不会遇到这个问题
	if !it.dataPageIter.SeekLe(target) {
		it.loadDataPage(it.pageIndex-1)
		it.dataPageIter.SeekLe(target)
	}
	it.valid = true
	return true
}

func (it *Iter)SeekGE(target base.InternalKey) (find bool) {
	// search sstIndex
	// load data page
	// position data page iter
	i, found := it.searchInIndexPage(target.UserKey)
	if !found {
		// 有两种情况
		// 1. 这不是第一个index-page, (但是当前的实现是一次性加载整个index)
		// 2. 所有record都大于target
		return false
	}

	it.loadDataPage(i)
	if it.dataPageIter.SeekGe(target) {
		it.valid = true
		return true
	} else {
		it.valid = false
		return false
	}
}

func (it *Iter)Next() bool {
	it.assertValid()

	if it.dataPageIter.Next() {
		return true
	} else {
		if it.pageIndex+ 1 > it.dataPageCount - 1 {
			return false
		} else {
			it.loadDataPage(it.pageIndex+1)
			it.dataPageIter.First()
			return true
		}
	}
}

// 副作用是修改it.pageIndex
func (it *Iter)loadDataPage(i int) {
	if it.dataPageIter != nil {
		it.cache.Free(it.sst.Meta.FileNum, it.pageIndex)
		it.dataPageIter.Close()
	}
	buf, found := it.cache.AskFor(it.sst.Meta.FileNum, i)
	if !found {
		it.sst.F.ReadAt(buf, int64(i*PageSize))
	}
	it.dataPageIter = NewDataPageIter(buf)
	it.dataPageIter.First()

	it.pageIndex = i
}

func (it *Iter)Get() base.Record {
	it.assertValid()
	return it.dataPageIter.Get()
}

func (it *Iter)freeCurrentDataPage() {
	it.cache.Free(it.sst.Meta.FileNum, it.pageIndex)
}

func (it *Iter)Close() {
	it.assertValid()
	it.freeCurrentDataPage()
	it.valid = false
	it.dataPageIter.Close() // 会不会重复Close()...
	it.dataPageIter = nil
}






