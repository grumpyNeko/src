package sstable

import (
	"encoding/json"
	"mydb/base"
	"os"
	"path"
	"time"
)

/*
4K/3/8 = 170
4*1024 - 170*8*3 = 16
0-1360-2720-4080
*/
type SSTBuilder struct {
	metaPage     *SectionBuilder
	indexBuilder *IndexBuilder
	indexes      []*IndexBuilder
	dataPage     *DataPageBuilder
	file         *os.File
	dbdir        string
	meta         *SSTMeta

	isDone       bool
}

func NewSSTBuilder(dbdir string, fn int, level int) *SSTBuilder {
	filename := base.SSTableFileName(fn)
	f, err := os.OpenFile(path.Join(dbdir, filename), os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		panic(err.Error())
	}
	// todo: 如果已存在?
	meta := &SSTMeta{
		FileNum:    fn,
		Created: 	0,
		Level:   	level,
		UkeyRange:  nil,
		TailRange:  nil,
		DataPageCount: 0,
	}
	return &SSTBuilder{
		metaPage:     NewSectionBuilder(make([]byte, PageSize, PageSize)),
		indexBuilder: NewIndexBuilder(1), // todo: ...
		dataPage:     NewDataPageBuilder(),
		file:         f,
		meta:         meta,
		indexes:      make([]*IndexBuilder, 0, 4),
		dbdir:        dbdir,
	}
}

func (b *SSTBuilder)Add(ikey base.InternalKey, val uint64) {
	/*
	   for i,ukey in sstIndex
	     if target <= ukey
	   	return i*PageSize
	*/
	b.assertNotDone()

	success, needMoreSpace := b.dataPage.AddRecord(ikey, val)
	if needMoreSpace {
		b.addIndexEntry(ikey.UserKey) // todo: 把最后一个key加入index, 导致不满一个page的sst的index-page是空的!
		b.dataPage.done()
		b.file.Write(b.dataPage.buf)
		//b.file.Sync()
		b.meta.DataPageCount++
		b.dataPage = NewDataPageBuilder()
		if !success { // 其实永远不会进入这里
			b.Add(ikey, val)
		}
	}
	b.updateKeyRange(ikey)
}

func (b *SSTBuilder)addIndexEntry(ukey uint64) {
	if !b.indexBuilder.WriteUint64(ukey) {
		b.indexBuilder.Done()
		b.indexes = append(b.indexes, b.indexBuilder)
		b.indexBuilder = NewIndexBuilder(1)

		b.indexBuilder.WriteUint64(ukey) // retry
	}
}

func (b *SSTBuilder)Done() *SST {
	b.assertNotDone()

	if b.dataPage.recordCount != 0 {
		b.dataPage.done()
		b.file.Write(b.dataPage.buf)
		b.meta.DataPageCount++
	}

	for j:=0; j<len(b.indexes); j++ {
		b.file.Write(b.indexes[j].buf)
	}
	b.file.Write(b.indexBuilder.buf)

	b.meta.Created = time.Now().UnixNano()

	res, err := json.Marshal(*b.meta)
	if err != nil {
		panic(err.Error())
	}
	copy(b.metaPage.buf, res)
	b.file.Write(b.metaPage.buf)

	b.file.Sync()
	filename := b.file.Name()
	b.file.Close()

	b.isDone = true


	readOnlyFile, err := os.OpenFile(filename, os.O_RDONLY, 0644) // filename包含path
	if err != nil {
		panic(err.Error())
	}
	return &SST{
		F:    readOnlyFile,
		Meta: b.meta,
	}
}

func (b *SSTBuilder)assertNotDone() {
	if b.isDone {
		panic("This SSTBuilder is already done!")
	}
}

func (b *SSTBuilder)updateKeyRange(ikey base.InternalKey) {
	b.assertNotDone()

	ukey := ikey.UserKey
	tail := ikey.Tail

	if b.meta.UkeyRange == nil { // SSTable的第一个record
		b.meta.UkeyRange = &base.Range{
			Start: ukey,
			End:   ukey,
		}

		b.meta.TailRange = &base.Range{
			Start: tail,
			End:   tail,
		}
	} else {
		b.meta.UkeyRange.Start = base.MinUint64(ukey, b.meta.UkeyRange.Start)
		b.meta.UkeyRange.End = base.MaxUint64(ukey, b.meta.UkeyRange.End)

		b.meta.TailRange.Start = base.MinUint64(tail, b.meta.TailRange.Start)
		b.meta.TailRange.End = base.MaxUint64(tail, b.meta.TailRange.End)
	}

}