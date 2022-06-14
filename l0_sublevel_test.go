package mydb

import (
	"testing"
)

func Test_MakeL0Sublevels(t *testing.T) {
	dbdir := "./testdata/common"
	db := Open(dbdir)
	db.memtables[0] = InputFileToMemtable(dbdir, 5)
	db.memtables[1] = InputFileToMemtable(dbdir, 6)
	s := MakeL0Sublevels(db.version.Levels[0].ArrayList)
	println(s)

	sublevelDeepthThreshold := 1

	maxDeepIndex := 0
	for i,interval := range s.Intervals {
		if len(interval.tables) > len(s.Intervals[maxDeepIndex].tables) {
			maxDeepIndex = i
		}
	}
	seedInterval := s.Intervals[maxDeepIndex]
	if len(seedInterval.tables) < sublevelDeepthThreshold {
		return
	}

	cp := db.getCompactPlanFromSeed(seedInterval.indexes)
	println(cp)
}



