package sstable

import (
	"fmt"
	"mydb/base"
	"mydb/cache"
	"os"
	"path"
	"testing"
)

const dbdir string = "../testdata/multi_table"

func Test_SSTableIter_Le(t *testing.T) {
	c := cache.NewFIFOCache(3)
	sstFile, _ := os.OpenFile(path.Join(dbdir, fmt.Sprintf("%d.sst", 0)),  os.O_RDONLY, 0644)
	iter := NewSSTableIter(&SST{
		F: sstFile,
		Meta: &SSTMeta{
			FileNum:       0,
			Created:       0,
			Level:         0,
			UkeyRange:     nil,
			TailRange:     nil,
			DataPageCount: 2,
		},
	}, c)
	// 这是一个特殊的case
	// 查index-page定位到page1
	// 但是目标record是page0的最后一个
	if iter.SeekLe(base.InternalKey{
		UserKey: 170,
		Tail:    0,
	}) {
		println(iter.Get().Pretty())
	}
	for iter.Next() {
		println(iter.Get().Pretty())
	}
}

func Test_SSTableIter_Ge(t *testing.T) {
	c := cache.NewFIFOCache(3)
	sstFile, _ := os.OpenFile(path.Join(dbdir, fmt.Sprintf("%d.sst", 0)),  os.O_RDONLY, 0644)
	iter := NewSSTableIter(&SST{
		F: sstFile,
		Meta: &SSTMeta{
			FileNum:       0,
			Created:       0,
			Level:         0,
			UkeyRange:     nil,
			TailRange:     nil,
			DataPageCount: 2,
		},
	}, c)
	if iter.SeekGE(base.InternalKey{
		UserKey: 339,
		Tail:    0,
	}) {
		println(iter.Get().Pretty()) // 339
	}
	for iter.Next() {
		println(iter.Get().Pretty())
	}
}