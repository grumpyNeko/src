package mydb

import "mydb/sstable"

type LevelMetaIter struct {
	level   *LevelMeta
	pos 	int
	valid   bool
}

func (l *LevelMetaIter)First() bool {
	if len(l.level.ArrayList) == 0 {// todo...
		return false
	}
	l.pos = 0
	l.valid = true
	return true
}

func (l *LevelMetaIter)Next() bool {
	if !l.valid {
		panic("iter is not valid")
	}
	if l.pos >= len(l.level.ArrayList) - 1 { // == 是不够的
		return false
	} else {
		l.pos ++
		return true
	}
}

func (l *LevelMetaIter)Get() *sstable.SST {
	if !l.valid {
		panic("iter is not valid")
	}
	return l.level.ArrayList[l.pos]
}