package sstable

import (
	"encoding/binary"
	"mydb/base"
)

type DataPageBuilder struct {
	buf   		[]byte
	ukeySection *SectionBuilder
	tvSection   *SectionBuilder
	recordCount uint16
}

func NewDataPageBuilder() *DataPageBuilder {
	// todo: maybe should reuse buf
	page := make([]byte, PageSize, PageSize)
	return &DataPageBuilder{
		buf:         page,
		recordCount: 0,
		ukeySection: NewSectionBuilder(page[:1360]),
		tvSection:   NewSectionBuilder(page[1360:4080]),
	}
}

/*
ukeySection, tvSectio <- ikey, val
recordCount ++
 */
func (b *DataPageBuilder)AddRecord(ikey base.InternalKey, val uint64) (success bool,  needMoreSpace bool) {
	if b.ukeySection.cur == 1360 {
		return false, true
	}
	// todo: 是否应该检查...
	b.ukeySection.WriteUint64(ikey.UserKey)
	b.tvSection.WriteUint64(ikey.Tail)
	b.tvSection.WriteUint64(val)
	b.recordCount++
	if b.ukeySection.cur == 1360 {
		return true, true
	} else {
		return true, false
	}
}

/*
b.buf[-2:] <: b.recordCount
 */
func (b *DataPageBuilder)done() {
	binary.LittleEndian.PutUint16(b.buf[len(b.buf)-2:], b.recordCount)
}

type SectionBuilder struct {
	buf   []byte
	cur   int
}

func NewSectionBuilder(buf []byte) *SectionBuilder {
	return &SectionBuilder{
		buf: buf,
		cur: 0,
	}
}

func (w *SectionBuilder)WriteUint64(i uint64) {
	binary.LittleEndian.PutUint64(w.buf[w.cur:], i)
	w.cur += 8
}