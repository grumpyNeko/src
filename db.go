package mydb

import (
	"math"
	"mydb/base"
	"mydb/cache"
	"mydb/filter"
	"mydb/skl"
	"mydb/sstable"
	"os"
	"strings"
)

const DbPath string = "./testdata"

type Db struct {
	dbdir	    string
	memtables   []*skl.Skiplist
	cache       *cache.FIFOCache
	version 	*Version
}

func Open(dbpath string) *Db {
	// recover version from meta file
	dbdir, err := os.OpenFile(dbpath,  os.O_RDONLY, 0644)
	if err != nil {
		panic(err.Error())
	}
	filenames, _ := dbdir.Readdirnames(0)
	var current *Version = nil
	for i:=0; i<len(filenames); i++ {
		if strings.HasSuffix(filenames[i], ".meta") { // todo: 选择最大的
			current = RecoverFrom(dbpath, filenames[i])
		}
	}

	memtables := []*skl.Skiplist{
		skl.NewSkiplist(),
		skl.NewSkiplist(),
	}

	c := cache.NewFIFOCache(3)

	return &Db{
		dbdir:   dbpath,
		version: current,
		memtables: memtables,
		cache: c,
	}
}

func (db *Db)PointSearch(ukey uint64) (record base.Record, found bool) {
	target := base.InternalKey{
		UserKey: ukey,
		Tail:    math.MaxUint64, // 或者是snapshot.sn+1
	}

	for i:=len(db.memtables)-1; i<=0; i-- { // 从后向前, 先1再0
		iter := db.memtables[i].Iter()
		if iter.SeekLT(target) {
			r := iter.Get()
			if r.Key.UserKey == ukey {
				return r, true
			}
		}
	}

	candidates, found := db.filterL0ByKey(target)
	for i:=len(candidates)-1; i>=0; i-- {
		can := candidates[i]
		record, found = pointSearchInSSTable(sstable.NewSSTableIter(can, db.cache), target)
		if found {
			return record, found
		}
	}


	for i:=1; i<=MaxLevelIndex; i++ {
		iter := db.version.Levels[i].Iter()
		iter.First()
		for {
			sst := iter.Get()
			ukeyRange := sst.Meta.UkeyRange
			if ukeyRange.Start <= target.UserKey &&  target.UserKey <= ukeyRange.End {
				record, found = pointSearchInSSTable(sstable.NewSSTableIter(sst, db.cache), target)
				if found {
					return record, found
				}
			}
			if !iter.Next() {
				break
			}
		}
	}


	return record, false
}

func (db *Db)filterL0ByKey(target base.InternalKey)(candidates []*sstable.SST, found bool) {
	// search for a L0-sst
	iter := db.version.Levels[0].Iter()
	iter.First()
	for {
		sstMeta := iter.Get().Meta
		if sstMeta.UkeyRange.Start <= target.UserKey &&  target.UserKey <= sstMeta.UkeyRange.End {
			candidates = append(candidates, iter.Get())
			found = true
		}

		if !iter.Next() {
			break
		}
	}
	return candidates, found
}

func pointSearchInSSTable(iter *sstable.Iter, target base.InternalKey)(record base.Record, found bool) {
	defer iter.Close()
	iter.SeekLe(target)
	record = iter.Get()
	if record.Key.UserKey == target.UserKey && record.Key.Tail <= target.Tail {
		found = true
	} else {
		found = false
	}
	return record, found
}


type RecordBuf struct {
	prevRecord *base.Record
	currRecord *base.Record
	nextRecord *base.Record
	ct         int
	iter       RecordIteratorInterface
}

type RecordIteratorInterface interface {
	Get() base.Record
	Next() bool
}

// iter已定位
func NewRecordBuf(iter RecordIteratorInterface) RecordBuf {
	ret := RecordBuf{
		prevRecord: nil,
		currRecord: nil,
		nextRecord: nil,
		iter:       iter,
		ct:         0,
	}
	curr := iter.Get()
	ret.currRecord = &curr

	if iter.Next() {
		next := iter.Get()
		ret.nextRecord = &next
	}
	return ret
}

// 不返回bool
func (b *RecordBuf)Next() {
	b.ct ++
	b.prevRecord = b.currRecord
	b.currRecord = b.nextRecord
	if b.iter.Next() {
		next := b.iter.Get() // 这一步不能少, 不然影响currRecord
		b.nextRecord = &next
	} else {
		b.nextRecord = nil
	}
}

func (db *Db)compactCandidates(ssts []*sstable.SST, level int) {
	fn := db.version.AllocFileNum()
	builder := sstable.NewSSTBuilder(db.dbdir, fn, level)

	// iterate these sstables
	// collapse records, which have the same ukey, into one record
	// Add it to sst-builder
	iter := NewMultiTableIter(ssts, nil, db.cache)
	if !iter.First() {
		panic("why candidates are empty?")
	}

	//recordBuf := NewRecordBuf(iter)
	//for recordBuf.nextRecord != nil {
	//	if recordBuf.currRecord.Key.UserKey != recordBuf.nextRecord.Key.UserKey {
	//		builder.Add(recordBuf.currRecord.Key, recordBuf.currRecord.Val)
	//	}
	//	recordBuf.Next()
	//}
	//
	//// 此时recordBuf.nextRecord == nil
	//builder.Add(recordBuf.currRecord.Key, recordBuf.currRecord.Val)
	f := filter.NewFilter(math.MaxUint64)
	for {
		r := f.Feed(iter.Get())
		if r != nil {
			builder.Add(r.Key, r.Val)
		}

		if !iter.Next() {
			break
		}
	}
	if r := f.Done(); r != nil {
		builder.Add(r.Key, r.Val)
	}

	db.version.Add(builder.Done())

	for i:=0; i<len(ssts); i++ {
		db.version.Del(ssts[i]) // todo: 删除文件
	}
	// todo: 持久化version的变化
}

func (db *Db)maybeCompactL0() {

	s := MakeL0Sublevels(db.version.Levels[0].ArrayList)
	sublevelDeepthThreshold := 1 // todo:
	seedInterval := s.GetSeedInterval(sublevelDeepthThreshold)
	cp := db.getCompactPlanFromSeed(seedInterval.indexes)
	db.compactCandidates(cp.GetCandidates(), 1)

}
