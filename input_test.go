package mydb

import (
	"mydb/base"
	skl "mydb/skl"
	"testing"
)

// todo: del
func Test_skl_to_inputfile(t *testing.T) {
	skl := skl.NewSkiplist()
	ukey := uint64(300)
	for i:=uint64(200169); i>=200001; i-- {
		skl.Add(base.InternalKey{
			UserKey: ukey,
			Tail:    i,
		}, i)
		ukey++
	}
	skl.Add(base.InternalKey{
		UserKey: 469,
		Tail:    200000,
	}, 200000)

	MemtableToInputFile("./testdata", 11, skl)
}

func Test_input_to_skl(t *testing.T) {
	mem := InputFileToMemtable("./testdata", 11)
	iter := mem.Iter()
	if !iter.First() {
		return
	}
	for {
		println(iter.Get().Pretty())
		if !iter.Next() {
			break
		}
	}
}