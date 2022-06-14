package mydb

import (
	"mydb/sstable"
)

type LevelMeta struct {
	ArrayList []*sstable.SST
}

func NewLevelMeta() *LevelMeta {
	return &LevelMeta{
		ArrayList: make([]*sstable.SST, 0, 16),
	}
}

func (l *LevelMeta)Add(sst sstable.SST) { // 不能是(l LevelMeta)
	l.ArrayList = append(l.ArrayList, &sst)
}

func (l *LevelMeta)Del(fn int) {
	for i:=0; i< len(l.ArrayList); i++ { // 没有使用Iter
		if l.ArrayList[i].Meta.FileNum == fn {
			// 需要ArrayList无序
			l.ArrayList[i] = l.ArrayList[len(l.ArrayList)-1]
			l.ArrayList = l.ArrayList[:len(l.ArrayList)-1]
		}
	}

}

func (l *LevelMeta)Iter() *LevelMetaIter {
	return &LevelMetaIter{
		level: l,
		pos:   -1,
	}
}

// 没有使用iter
func (l *LevelMeta)DeepCopy() LevelMeta {
	lenOfList := len(l.ArrayList)

	ret := LevelMeta{
		ArrayList: make([]*sstable.SST, lenOfList, lenOfList),
	}

	for i:=0;i<lenOfList; i++ {
		newSST := l.ArrayList[i].DeepCopy()
		ret.ArrayList[i] = &newSST
	}

	return ret
}

