package mydb

import (
	"fmt"
	"mydb/sstable"
	"testing"
)

func Test_PointSearch(t *testing.T) {
	dbdir := "./testdata/common"
	db := Open(dbdir)
	db.memtables[0] = InputFileToMemtable(dbdir, 5)
	db.memtables[1] = InputFileToMemtable(dbdir, 6)
	record, find := db.PointSearch(2)
	if find {
		println(record.Pretty())
	} else {
		println("not found")
	}
}

func Test_compactCandidates(t *testing.T) {
	db := Open("./testdata/compact")

	level0 := db.version.Levels[0].ArrayList
	can := make([]*sstable.SST, 0, 16)
	for _, sst := range level0 {
		if sst.Meta.FileNum == 1 || sst.Meta.FileNum == 2 {
			can = append(can, sst)
		}
	}
	can = append(can, db.version.Levels[1].ArrayList[0])
	db.compactCandidates(can, 1)
	detail := db.version.Levels[1].ArrayList[0].GetDetail() // todo: illegal types for operand: print
	fmt.Print(detail)
}

