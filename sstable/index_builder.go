package sstable

import (
	"encoding/binary"
)

type IndexBuilder struct {
	isDone bool
	buf    []byte
	cur    int
}

func NewIndexBuilder(n int) *IndexBuilder {
	return &IndexBuilder{
		isDone: false,
		buf: make([]byte, n*PageSize, n*PageSize), // 不扩容, 必要时缩减
		cur: 0,
	}
}

func (b *IndexBuilder)WriteUint64(n uint64) bool  {
	if b.isDone {
		panic("IndexBuilder is already done!")
	}
	if b.cur >= PageSize {
		return false
	}
	binary.LittleEndian.PutUint64(b.buf[b.cur:], n)
	b.cur += 8
	return true
}

func (b *IndexBuilder)Done() {
	if b.isDone {
		panic("IndexBuilder is already done!")
	}
	b.isDone = true
	emtpyPageNum := (cap(b.buf) - b.cur) / PageSize
	b.buf = b.buf[:cap(b.buf)-emtpyPageNum*PageSize] // todo: 这样合适吗, 会不会浪费内存
}

