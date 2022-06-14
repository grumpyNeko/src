package mydb

import (
	"mydb/base"
	"mydb/sstable"
	"sort"
)


type CompactPlan struct {
	selected 		map[*sstable.SST]bool
	overallRange   *base.Range
	totalSize		int // measured in DataPageCount
}

func (db *Db)getCompactPlanFromSeed(seeds []int) *CompactPlan {
	if len(seeds) == 0 {
		panic("...")
	}
	l0 := db.version.Levels[0].DeepCopy().ArrayList
	l1 := db.version.Levels[1].DeepCopy().ArrayList
	plan := CompactPlan{
		selected:     make(map[*sstable.SST]bool, 16),
		overallRange: nil,
		totalSize:     0,
	}

	// 按FileNum增序
	sort.Slice(l0, func(i, j int) bool {
		return l0[i].Meta.FileNum < l0[j].Meta.FileNum
	})
	// 按FileNum增序
	sort.Slice(l1, func(i, j int) bool {
		return l1[i].Meta.UkeyRange.Start < l1[j].Meta.UkeyRange.Start
	})


	for _,seedIndex := range seeds {
		grow := plan.addL0(l0[seedIndex]) // 其实传*SST跟传index本质上一样
		if grow {
			plan.holdInvariant(l0[:seedIndex]) // 这就是为什么穿seedIndex而不是SST对象
		}
	}


	for _,sst := range l1 { // todo: 只检查一个片段
		if plan.overallRange.HasOverlap(*sst.Meta.UkeyRange) {
			grow := plan.addL1(sst)
			if grow {
				plan.holdInvariant(l0) // L1不重叠, 只需检查L0
			}
		}
	}

	return &plan
}

func (cp *CompactPlan)addL0(sst *sstable.SST) (grow bool) {
	if _,ok := cp.selected[sst]; ok {
		return false
	}

	cp.selected[sst] = true
	// overallRange0
	if cp.overallRange == nil {
		cp.overallRange = sst.Meta.UkeyRange
		grow = true
	} else {
		grow = !cp.overallRange.Contain(*sst.Meta.UkeyRange)
		if grow {
			*cp.overallRange = cp.overallRange.Union(*sst.Meta.UkeyRange)
		}
	}
	// totalSize
	cp.totalSize += sst.Meta.DataPageCount
	return grow
}

func (cp *CompactPlan)addL1(sst *sstable.SST) (grow bool) {
	if _,ok := cp.selected[sst]; ok {
		return false
	}

	cp.selected[sst] = true

	grow = !cp.overallRange.Contain(*sst.Meta.UkeyRange)
	if grow {
		*cp.overallRange = cp.overallRange.Union(*sst.Meta.UkeyRange)
	}

	// totalSize
	cp.totalSize += sst.Meta.DataPageCount
	return grow
}

func (cp *CompactPlan)holdInvariant(olders []*sstable.SST) {
	size := len(olders)
	if size == 0 {
		return
	}

	for i:=size-1; i>=0; i-- {
		sst := olders[i]
		if sst.Meta.UkeyRange.HasOverlap(*cp.overallRange) {
			cp.addL0(sst)
		}
	}
}

func (cp *CompactPlan)GetCandidates() []*sstable.SST {
	ret := make([]*sstable.SST, 0, 16)
	for key,_ := range cp.selected {
		ret = append(ret, key)
	}
	return ret
}