package mydb

import (
	"sort"
	"mydb/sstable"
)

type Interval struct {
	//index        int
	ukey         uint64
	//isCompacting bool
	tables       []*sstable.SST
	indexes      []int
}

type L0Sublevels struct {
	l0 			    []*sstable.SST
	Intervals 		[]*Interval	// In increasing key order.
}

func MakeL0Sublevels(l0 []*sstable.SST) *L0Sublevels {
	sublevel := &L0Sublevels{
		l0:        l0,
		Intervals: nil,
	}

	keys := make([]uint64, 0, 2*len(l0))
	for _,sst := range l0 {
		ukeyRange := sst.Meta.UkeyRange
		keys = append(keys, ukeyRange.Start)
		keys = append(keys, ukeyRange.End+1) // immediate successor
	}

	intervals := keysToIntervals(keys)

	for i,sst := range l0 {
		i1, i2 := searchIntervals(intervals, sst.Meta.UkeyRange.Start, sst.Meta.UkeyRange.End)
		if i2 <= i1 {
			panic("There is no way i2 is equal to i1")
		}
		i2-- // sst is not in i2
		for j:=i1; j<=i2; j++ {
			interval := intervals[j]
			interval.tables = append(interval.tables, sst)
			interval.indexes = append(interval.indexes, i)
		}
		sst.Meta.SmallestSublevelIndex, sst.Meta.LargestSublevelIndex = i1, i2
	}

	sublevel.Intervals = intervals
	return sublevel
}

func searchIntervals(intervals []*Interval, start uint64, end uint64)  (i1 int, i2 int) {
	i := 0
	j := len(intervals)-1
	target := start
	var mid int
	for j >= i {
		mid = i + (j-i)/2
		if intervals[mid].ukey == target {
			break
		} else if intervals[mid].ukey > target {
			j = mid - 1
		} else {
			i = mid + 1
		}
	}
	i1 = mid

	i = i1
	j = len(intervals)-1
	target = end+1
	for j >= i {
		mid = i + (j-i)/2
		if intervals[mid].ukey == target {
			break
		} else if intervals[mid].ukey > target {
			j = mid - 1
		} else {
			i = mid + 1
		}
	}
	i2 = mid

	return i1, i2
}

func (sl *L0Sublevels)GetSeedInterval(sublevelDeepthThreshold int) *Interval {
	maxDeepIndex := 0
	for i,interval := range sl.Intervals {
		if len(interval.tables) > len(sl.Intervals[maxDeepIndex].tables) {
			maxDeepIndex = i
		}
	}
	seedInterval := sl.Intervals[maxDeepIndex]
	if len(seedInterval.tables) < sublevelDeepthThreshold {
		return nil
	}
	return seedInterval
}

type keySorter struct {
	keys []uint64
}
func (s keySorter) Len() int { return len(s.keys) }
func (s keySorter) Less(i, j int) bool {
	return s.keys[i] < s.keys[j]
}
func (s keySorter) Swap(i, j int) {
	s.keys[i], s.keys[j] = s.keys[j], s.keys[i]
}

func keysToIntervals(keys []uint64) []*Interval {

	// sort and dedup
	if len(keys) == 0 {
		return nil
	}
	sort.Sort(keySorter{keys: keys})
	j := 0
	for i := 1; i < len(keys); i++ {
		if keys[i] != keys[j] {
			j++
			keys[j] = keys[i]
		}
	}


	intervals := make([]*Interval, j+1, j+1)
	for i:=0; i<=j; i++ {
		intervals[i] = &Interval{
			ukey:         keys[i],
			tables:       make([]*sstable.SST, 0, 8),
			indexes:      make([]int, 0, 8),
		}
	}
	return intervals
}


