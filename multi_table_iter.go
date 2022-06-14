package mydb

import (
	"mydb/base"
	cache "mydb/cache"
	"mydb/skl"
	"mydb/sstable"
)

type MultiTableIter struct {
	iters    []base.TableIter
	minKey   base.InternalKey
	minIndex int
	valid       bool
}


func NewMultiTableIter(ssts []*sstable.SST, mems []*skl.Skiplist, c *cache.FIFOCache) *MultiTableIter {
	iters := make([]base.TableIter, 0, len(ssts)+len(mems))
	for _,sst := range ssts {
		iters = append(iters, sstable.NewSSTableIter(sst, c))
	}
	for _,mem := range mems {
		iters = append(iters, mem.Iter())
	}
	return &MultiTableIter{
		iters:     iters,
		minKey:    base.InternalKey{
			UserKey: base.MaximumUint64,
			Tail:    base.MaximumUint64,
		},
		minIndex:  -1,
		valid:     false,
	}
}

func (it *MultiTableIter)assertValid() {
	if !it.valid {
		panic("MultiTableIter is not valid")
	}
}

func (it *MultiTableIter)First() bool {
	if len(it.iters) == 0 {
		return false
	}

	for _,iter := range it.iters {
		iter.First()
	}
	it.minIndex, it.minKey = findMin(it.iters)

	it.valid = true
	return true
}

//func (it *MultiTableIter)SeekLe(ikey InternalKey) bool {
//	it.valid = true
//
//	for i,iter := range it.iters {
//		ok := iter.SeekLe(ikey)
//		if !ok {
//			// remove iter which fails to SeekLe(ikey)
//			// this make MultiTableIter non-reusable
//			it.iters[i] = it.iters[len(it.iters)-1]
//			it.iters = it.iters[:len(it.iters)-1]
//		}
//	}
//	if len(it.iters) == 0 {
//		return false
//	}
//
//	// 找到所有iter里最大的那个, 其他的全部后移
//	// 被后移的iter现在全都大于ikey(或者为空)
//	maxIndex, _ := findMax(it.iters)
//	for i,sst := range it.iters {
//		if i != maxIndex {
//			ok := sst.Next()
//			if !ok {// Del
//				it.iters[i] = it.iters[len(it.iters)-1]
//				it.iters = it.iters[:len(it.iters)-1]
//			}
//		}
//	}
//
//	it.minIndex, it.minKey = findMin(it.iters)
//	return true
//}

func (it *MultiTableIter)SeekGe(ikey base.InternalKey) bool {
	it.valid = true

	for i,iter := range it.iters {
		ok := iter.SeekGE(ikey)
		if !ok {
			// remove iter which fails to SeekLe(ikey)
			// this make MultiTableIter non-reusable
			it.iters[i] = it.iters[len(it.iters)-1]
			it.iters = it.iters[:len(it.iters)-1]
		}
	}
	if len(it.iters) == 0 {
		return false
	}

	// 不需要像Le一样后移, 因为我们需要一个尽量小的

	it.minIndex, it.minKey = findMin(it.iters)
	return true
}

func (it *MultiTableIter)Next() bool {
	it.assertValid()

	if len(it.iters) == 0 {
		it.minIndex = -1
		it.minKey = base.InternalKey{
			UserKey: base.MaximumUint64,
			Tail:    base.MaximumUint64,
		}
		return false
	}

	if !it.iters[it.minIndex].Next() {
		it.removeIter(it.minIndex)
	}
	if len(it.iters) == 0 {
		return false
	}
	it.minIndex, it.minKey = findMin(it.iters)

	return true
}

func (it *MultiTableIter)removeIter(index int) {
	it.iters[index].Close()
	it.iters[index] = it.iters[len(it.iters)-1]
	it.iters = it.iters[:len(it.iters)-1]
}

func (it *MultiTableIter)Get() base.Record {
	it.assertValid()

	return it.iters[it.minIndex].Get()
}

func (it *MultiTableIter)Close() {
	for _,iter := range it.iters {
		iter.Close()
	}
	it.iters = nil
	it.minIndex = -1
	it.minKey = base.InternalKey{
		UserKey: base.MaximumUint64,
		Tail:    base.MaximumUint64,
	}
	it.valid = false
}

func findMax(iters []base.TableIter) (maxIndex int, maxKey base.InternalKey) {
	maxIndex = -1
	maxKey = base.InternalKey{
		UserKey: 0,
		Tail:    0,
	}

	for j:=0; j<len(iters); j++ {
		currentKey := iters[j].Get().Key
		if base.Compare(currentKey, maxKey) > 0 {
			maxKey = currentKey
			maxIndex = j
		}
	}

	return maxIndex, maxKey
}

func findMin(iters []base.TableIter) (minIndex int, minKey base.InternalKey) {
	minIndex = -1
	minKey = base.InternalKey{
		UserKey: base.MaximumUint64,
		Tail:    base.MaximumUint64,
	}

	for j:=0; j< len(iters); j++ {
		currentKey := iters[j].Get().Key
		if base.Compare(currentKey, minKey) < 0 {
			minKey = currentKey
			minIndex = j
		}
	}

	return minIndex, minKey
}

