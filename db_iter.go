package mydb

import (
	"mydb/base"
	"mydb/sstable"
)

type DbIter struct {
	// todo: memtables
	diskIter   *MultiTableIter
	valid      bool
}

func (db *Db)Iter() *DbIter {
	levels := db.version.Levels
	arrayList := make([]*sstable.SST, 0, 16)
	for i:=0; i<len(levels); i++ {
		level := levels[i]
		arrayList = append(arrayList, level.ArrayList...)
	}
	return &DbIter{
		diskIter: NewMultiTableIter(arrayList, nil, db.cache),
		valid: false,
	}
}

func (it *DbIter)First() bool {
	return it.diskIter.First()
}

func (it *DbIter)SeekGe(ikey base.InternalKey) bool {
	return it.diskIter.SeekGe(ikey)
}

//func (it *DbIter)SeekLe(ikey base.InternalKey) bool {
//	return it.diskIter.SeekLe(ikey)
//}

func (it *DbIter)Get() base.Record {
	return it.diskIter.Get()
}

func (it *DbIter)Next() bool {
	return it.diskIter.Next()
}