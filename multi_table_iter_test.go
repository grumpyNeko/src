package mydb

import (
	"math"
	"os"
	"path"
	"mydb/base"
	"mydb/cache"
	"mydb/filter"
	skl "mydb/skl"
	"mydb/sstable"
	"testing"
)

//func Test_SeekLe(t *testing.T) {
//	file0, _ := os.OpenFile(path.Join("./testdata", "0.sst"),  os.O_RDONLY, 0644)
//	sst0 := &SST{
//		f:    file0,
//		Meta: &SSTMeta{
//			UkeyRange: &Range{
//				Start: 0,
//				End:   339,
//			},
//			TailRange: &Range{
//				Start: 0,
//				End:   4278190419,
//			},
//			Level: 0,
//			DataPageCount: 2,
//			FileNum: 0,
//		},
//	}
//
//	file1, _ := os.OpenFile(path.Join("./testdata", "1.sst"),  os.O_RDONLY, 0644)
//	sst1 := &SST{
//		f:    file1,
//		Meta: &SSTMeta{
//			UkeyRange: &Range{
//				Start: 0,
//				End:   87379,
//			},
//			TailRange: &Range{
//				Start: 0,
//				End:   4278277629,
//			},
//			Level: 0,
//			DataPageCount: 513,
//			FileNum: 1,
//		},
//	}
//
//	start := InternalKey{
//		UserKey: 339,
//		Tail:    0,
//	}
//	end := InternalKey{
//		UserKey: 339,
//		Tail:    MaximumUint64,
//	}
//	iter := NewMultiTableIter([]*SST{sst0, sst1}, nil)
//	iter.SeekLe(start) // todo: ge
//	for {
//		println(iter.Get().Pretty())
//		println(iter.ssts[iter.minIndex].sst.Meta.FileNum)
//		println()
//
//		if !iter.Next() || Compare(iter.Get().Key, end) > 0 {
//			break
//		}
//	}
//}

func prepareTables() (ssts []*sstable.SST, mems []*skl.Skiplist) {
	file0, _ := os.OpenFile(path.Join("./testdata/multi_table", "0.sst"),  os.O_RDONLY, 0644)
	sst0 := &sstable.SST{
		F:    file0,
		Meta: &sstable.SSTMeta{
			UkeyRange: &base.Range{
				Start: 0,
				End:   339,
			},
			TailRange: &base.Range{
				Start: 0,
				End:   4278190419,
			},
			Level: 0,
			DataPageCount: 2,
			FileNum: 0,
		},
	}

	file1, _ := os.OpenFile(path.Join("./testdata/multi_table", "1.sst"),  os.O_RDONLY, 0644)
	sst1 := &sstable.SST{
		F:    file1,
		Meta: &sstable.SSTMeta{
			UkeyRange: &base.Range{
				Start: 0,
				End:   87379,
			},
			TailRange: &base.Range{
				Start: 0,
				End:   4278277629,
			},
			Level: 0,
			DataPageCount: 513,
			FileNum: 1,
		},
	}

	mem0 := skl.NewSkiplist()
	mem0.Add(base.InternalKey{
		UserKey: 338,
		Tail:    5000000000,
	}, 677)
	mem0.Add(base.InternalKey{
		UserKey: 338,
		Tail:    5000000001,
	}, 0)

	return []*sstable.SST{sst0,sst1}, []*skl.Skiplist{mem0}
}

func Test_SeekGe(t *testing.T) {
	ssts, mems := prepareTables()

	start := base.InternalKey{
		UserKey: 338,
		Tail:    0,
	}
	end := base.InternalKey{
		UserKey: 339,
		Tail:    base.MaximumUint64,
	}

	c  := cache.NewFIFOCache(3)
	iter := NewMultiTableIter(ssts, mems, c)
	iter.SeekGe(start)
	for {
		println(iter.Get().Pretty())
		println()

		if !iter.Next() || base.Compare(iter.Get().Key, end) > 0 {
			break
		}
	}
	iter.Close()
}

func Test_SeekGe_filter(t *testing.T) {
	ssts, mems := prepareTables()

	start := base.InternalKey{
		UserKey: 338,
		Tail:    0,
	}
	end := base.InternalKey{
		UserKey: 339,
		Tail:    base.MaximumUint64,
	}

	c  := cache.NewFIFOCache(3)
	iter := NewMultiTableIter(ssts, mems, c)
	iter.SeekGe(start)
	f := filter.NewFilter(math.MaxUint64)
	for {
		r := f.Feed(iter.Get())
		if r != nil {
			println(r.Pretty())
		}

		if !iter.Next() || base.Compare(iter.Get().Key, end) > 0 {
			break
		}
	}
	iter.Close()
	println(f.Done().Pretty())
}